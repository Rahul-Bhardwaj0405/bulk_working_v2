import pandas as pd
import logging
from celery import shared_task
from .models import BookingTransaction, RefundTransaction
from io import BytesIO
import csv

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

BANK_CODE_MAPPING = {
    'hdfc': 101,
    'icici': 102,
    'karur_vysya': 40,
}

BANK_MAPPINGS = {
    'karur_vysya': {
        'booking': {
            'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'irctc_order_no',
                'BANKBOOKINGREFNO': 'bank_booking_ref_no',
                'BOOKINGAMOUNT': 'booking_amount',
                'TXNDATE': 'transaction_date',
                'CREDITEDON': 'credited_date'
            }
        },
        'refund': {
            'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'irctc_order_no',
                'REFUNDAMOUNT': 'refund_amount',
                'DEBITEDON': 'debited_date',
                'REFUNDDATE': 'refund_date',
                'BANKBOOKINGREFNO': 'bank_booking_ref_no',
                'BANKREFUNDREFNO': 'bank_refund_ref_no'
            }
        },
    }
}

def clean_column_name(column_name):
    parts = column_name.split()
    cleaned_name = ''.join(part for part in parts if part)
    cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()
    logger.debug(f"Cleaned column name: '{column_name}' to '{cleaned_name}'")
    return cleaned_name

def convert_to_int(val):
    try:
        if pd.isna(val) or val == '':
            return None
        return int(float(val))
    except (ValueError, OverflowError) as e:
        logger.error(f"Error converting value to int: {val}. Error: {e}")
        return None

@shared_task
def process_uploaded_files(files, bank_name, transaction_type, file_format):
    try:
        for file_index, file_content in enumerate(files):
            file_name = f"File-{file_index + 1}"  # Retrieve actual filename if available
            logger.info(f"Received task to process file: {file_name} of type {transaction_type}")

            # Reading the file content in batches
            if file_format == 'excel':
                logger.debug(f"Reading file {file_name} as Excel.")
                df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
                total_rows = df.shape[0]
                logger.debug(f"Initial DataFrame shape for {file_name}: {df.shape}")
                if total_rows == 0:
                    logger.error(f"No data found in the file {file_name}.")
                    return  # Exit if no data

                chunk_size = 50000
                
                for start_row in range(0, total_rows, chunk_size):
                    df_chunk = df.iloc[start_row:start_row + chunk_size]
                    logger.debug(f"Processing chunk from rows {start_row} to {start_row + chunk_size} for {file_name}.")
                    process_dataframe_chunk(df_chunk, bank_name, transaction_type)

            elif file_format == 'csv':
                logger.debug(f"Reading file {file_name} as CSV.")
                for chunk in pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL, chunksize=50000):
                    logger.debug(f"Processing a new CSV chunk of shape: {chunk.shape} for {file_name}.")
                    # Check if the chunk is empty
                    if chunk.empty:
                        logger.error(f"Empty chunk detected for file {file_name}. Skipping.")
                        continue  # Skip to the next chunk
                    process_dataframe_chunk(chunk, bank_name, transaction_type)

            else:
                logger.error(f"Unsupported file format: {file_format} for {file_name}")
                raise ValueError("Unsupported file format")

            logger.info(f"File processing completed for {file_name}")

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        raise  # Raise the exception for further handling/alerting

def process_dataframe_chunk(df_chunk, bank_name, transaction_type):
    # Clean column names
    cleaned_columns = [clean_column_name(col) for col in df_chunk.columns]
    logger.debug(f"Cleaned columns: {cleaned_columns}")

    # Assign cleaned column names back to DataFrame
    df_chunk.columns = cleaned_columns

    # Log DataFrame columns post cleaning and before renaming
    logger.debug(f"Columns before renaming: {df_chunk.columns.tolist()}")

    # Get the column mappings for booking and refund based on the bank name
    booking_col_mapping = BANK_MAPPINGS[bank_name]['booking']['column_mapping']
    refund_col_mapping = BANK_MAPPINGS[bank_name]['refund']['column_mapping']
    
    # Log mappings
    logger.debug(f"Retrieved booking column mapping for {bank_name}: {booking_col_mapping}")
    logger.debug(f"Retrieved refund column mapping for {bank_name}: {refund_col_mapping}")
    
    # Rename columns based on the mappings after cleaning
    df_chunk.rename(columns={
        'IRCTCORDERNO': booking_col_mapping['IRCTCORDERNO'],
        'BANKBOOKINGREFNO': booking_col_mapping['BANKBOOKINGREFNO'],
        'BOOKINGAMOUNT': booking_col_mapping['BOOKINGAMOUNT'],
        'TXNDATE': booking_col_mapping['TXNDATE'],
        'CREDITEDON': booking_col_mapping['CREDITEDON'],
        **{k: v for k, v in refund_col_mapping.items() if k in df_chunk.columns}  # Adding refund mappings only available in df_chunk
    }, inplace=True)
    
    # Log columns after renaming
    logger.debug(f"Columns after renaming: {df_chunk.columns.tolist()}")

    # Common columns
    common_columns = ['irctc_order_no', 'bank_booking_ref_no']  # Use renamed columns

    # Drop rows missing essential identifiers
    df_chunk.dropna(subset=common_columns, inplace=True)
    logger.debug(f"After dropping NAs, remaining rows: {df_chunk.shape[0]}")

    if df_chunk.empty:
        logger.warning("No valid rows remaining after dropping NAs. Skipping this chunk.")
        return
    logger.debug(f"DataFrame after dropping NAs:\n{df_chunk}")

    bulk_data_bookings = []
    bulk_data_refunds = []

    # Sets to track duplicates
    seen_booking_orders = set()
    seen_refund_orders = set()

    # Check required booking columns against the renamed df_chunk
    expected_booking_columns = ['transaction_date', 'irctc_order_no', 'bank_booking_ref_no', 'booking_amount', 'credited_date']
    has_required_booking_columns = all(col in df_chunk.columns for col in expected_booking_columns)
    logger.debug(f"Checking booking columns availability: {has_required_booking_columns}")

    if transaction_type in ['both', 'booking'] and has_required_booking_columns:
        logger.info("Processing booking transactions.")
        for index, row in df_chunk.iterrows():
            irctc_order_no = convert_to_int(row['irctc_order_no'])
            bank_booking_ref_no = convert_to_int(row['bank_booking_ref_no'])
            logger.debug(f"Row {index} data: IRCTC Order No = {irctc_order_no}, Bank Booking Ref No = {bank_booking_ref_no}")

            if irctc_order_no in seen_booking_orders or bank_booking_ref_no in seen_booking_orders:
                logger.warning(f"Duplicate booking transaction detected for order {irctc_order_no}. Skipping.")
                continue

            transaction_data = {
                'bank_code': BANK_CODE_MAPPING.get(bank_name),
                'irctc_order_no': irctc_order_no,
                'bank_booking_ref_no': bank_booking_ref_no,
                'booking_amount': float(row.get('booking_amount', 0.0)),
                'transaction_date': pd.to_datetime(row.get('transaction_date'), errors='coerce'),
                'credited_date': pd.to_datetime(row.get('credited_date'), errors='coerce'),
            }

            if transaction_data['irctc_order_no'] and transaction_data['bank_booking_ref_no']:
                bulk_data_bookings.append(transaction_data)
                seen_booking_orders.add(irctc_order_no)

    if transaction_type in ['both', 'refund']:
        expected_refund_columns = ['refund_date', 'irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no', 'refund_amount', 'debited_date']
        has_required_refund_columns = all(col in df_chunk.columns for col in expected_refund_columns)
        logger.debug(f"Checking refund columns availability: {has_required_refund_columns}")

        if has_required_refund_columns:
            logger.info("Processing refund transactions.")
            for index, row in df_chunk.iterrows():
                irctc_order_no = convert_to_int(row['irctc_order_no'])
                bank_refund_ref_no = convert_to_int(row['bank_refund_ref_no'])

                if irctc_order_no in seen_refund_orders or bank_refund_ref_no in seen_refund_orders:
                    logger.warning(f"Duplicate refund transaction detected for order {irctc_order_no}. Skipping.")
                    continue

                transaction_data = {
                    'bank_code': BANK_CODE_MAPPING.get(bank_name),
                    'irctc_order_no': irctc_order_no,
                    'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
                    'bank_refund_ref_no': bank_refund_ref_no,
                    'refund_amount': float(row.get('refund_amount', 0.0)),
                    'refund_date': pd.to_datetime(row.get('refund_date'), errors='coerce'),
                    'debited_date': pd.to_datetime(row.get('debited_date'), errors='coerce'),
                }

                if transaction_data['irctc_order_no'] and transaction_data['bank_refund_ref_no']:
                    bulk_data_refunds.append(transaction_data)
                    seen_refund_orders.add(irctc_order_no)

    if bulk_data_bookings:
        logger.debug(f"Bulk inserting {len(bulk_data_bookings)} booking transactions.")
        BookingTransaction.bulk_create_booking_transactions(bulk_data_bookings)

    if bulk_data_refunds:
        logger.debug(f"Bulk inserting {len(bulk_data_refunds)} refund transactions.")
        RefundTransaction.bulk_create_refund_transactions(bulk_data_refunds)

    logger.info(f"Processed {len(bulk_data_bookings)} booking transactions and {len(bulk_data_refunds)} refund transactions.")



# import pandas as pd
# import logging
# from celery import shared_task
# from .models import BookingTransaction, RefundTransaction
# from io import BytesIO
# import csv

# # Configure logging
# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler()
#     ]
# )

# logger = logging.getLogger(__name__)

# BANK_CODE_MAPPING = {
#     'hdfc': 101,
#     'icici': 102,
#     'karur_vysya': 40,
# }

# BANK_MAPPINGS = {
#     'karur_vysya': {
#         'booking': {
#             'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BOOKINGAMOUNT': 'booking_amount',
#                 'TXNDATE': 'transaction_date',
#                 'CREDITEDON': 'credited_date'
#             }
#         },
#         'refund': {
#             'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'REFUNDAMOUNT': 'refund_amount',
#                 'DEBITEDON': 'debited_date',
#                 'REFUNDDATE': 'refund_date',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BANKREFUNDREFNO': 'bank_refund_ref_no'
#             }
#         },
#         'both': {
#             'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON','REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BOOKINGAMOUNT': 'booking_amount',
#                 'TXNDATE': 'transaction_date',
#                 'CREDITEDON': 'credited_date',
#                 'IRCTCORDERNO': 'irctc_order_no',
#                 'REFUNDAMOUNT': 'refund_amount',
#                 'DEBITEDON': 'debited_date',
#                 'REFUNDDATE': 'refund_date',
#                 'BANKBOOKINGREFNO': 'bank_booking_ref_no',
#                 'BANKREFUNDREFNO': 'bank_refund_ref_no'
#             }

#         }
#     }
# }

# def clean_column_name(column_name):
#     parts = column_name.split()
#     cleaned_name = ''.join(part for part in parts if part)
#     cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()
#     logger.debug(f"Cleaned column name: '{column_name}' to '{cleaned_name}'")
#     return cleaned_name

# def convert_to_int(val):
#     try:
#         if pd.isna(val) or val == '':
#             return None
#         return int(float(val))
#     except (ValueError, OverflowError) as e:
#         logger.error(f"Error converting value to int: {val}. Error: {e}")
#         return None
# @shared_task
# def process_uploaded_files(file_content, file_name, bank_name, transaction_type, file_format):
#     try:
#         logger.info(f"Received task to process file: {file_name} of type {transaction_type}")

#         # Reading the file content
#         if file_format == 'excel':
#             logger.debug("Reading file as excel.")
#             df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
#         elif file_format == 'csv':
#             logger.debug("Reading file as CSV.")
#             df = pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         else:
#             logger.error(f"Unsupported file format: {file_format}")
#             raise ValueError("Unsupported file format")

#         logger.info(f"Initial columns found in file {file_name}: {', '.join(df.columns)}")

#         df.columns = [clean_column_name(col) for col in df.columns]
#         logger.info(f"Cleaned DataFrame columns: {list(df.columns)}")

#         # Initialize bulk data lists at the start
#         bulk_data_bookings = []
#         bulk_data_refunds = []

#         # If transaction_type is 'both', check if both sets of booking and refund columns exist
#         if transaction_type == 'both':
#             booking_columns = BANK_MAPPINGS[bank_name]['booking']['columns']
#             refund_columns = BANK_MAPPINGS[bank_name]['refund']['columns']
#             booking_columns_cleaned = [clean_column_name(col) for col in booking_columns]
#             refund_columns_cleaned = [clean_column_name(col) for col in refund_columns]
            
#             # Check for the presence of booking and refund columns
#             has_booking_columns = any(col in df.columns for col in booking_columns_cleaned)
#             has_refund_columns = any(col in df.columns for col in refund_columns_cleaned)

#             # Process the booking transactions if booking columns are present
#             if has_booking_columns:
#                 logger.info("Booking columns present in the file. Processing booking transactions.")
#                 for index, row in df.iterrows():
#                     transaction_data = {'bank_code': BANK_CODE_MAPPING.get(bank_name)}
#                     try:
#                         transaction_data.update({
#                             'transaction_date': pd.to_datetime(row.get('transaction_date'), errors='coerce'),
#                             'credited_date': pd.to_datetime(row.get('credited_date'), errors='coerce'),
#                             'booking_amount': float(row.get('booking_amount')) if pd.notnull(row.get('booking_amount')) else 0.0,
#                             'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
#                             'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
#                         })
#                         logger.debug(f"Row {index}: Transaction data for booking: {transaction_data}")

#                         # Ensure transaction data is valid before adding to bulk
#                         if transaction_data['irctc_order_no'] and transaction_data['bank_booking_ref_no'] is not None:
#                             bulk_data_bookings.append(transaction_data)
#                             logger.info(f"Adding booking transaction: {transaction_data}")

#                     except KeyError as key_err:
#                         logger.error(f"Missing key in booking row {index} with data {row.to_dict()}: {str(key_err)}")
#                     except Exception as ex:
#                         logger.error(f"Unexpected error processing booking row {index} with data {row.to_dict()}: {str(ex)}")

#             # Process the refund transactions if refund columns are present
#             if has_refund_columns:
#                 logger.info("Refund columns present in the file. Processing refund transactions.")
#                 for index, row in df.iterrows():
#                     transaction_data = {'bank_code': BANK_CODE_MAPPING.get(bank_name)}
#                     try:
#                         transaction_data.update({
#                             'refund_date': pd.to_datetime(row.get('refund_date'), errors='coerce'),
#                             'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
#                             'bank_refund_ref_no': convert_to_int(row.get('bank_refund_ref_no')),
#                             'refund_amount': float(row.get('refund_amount')) if pd.notnull(row.get('refund_amount')) else 0.0,
#                             'debited_date': pd.to_datetime(row.get('debited_date'), errors='coerce'),
#                             'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
#                         })
#                         logger.debug(f"Row {index}: Transaction data for refund: {transaction_data}")

#                         # Ensure transaction data is valid before adding to bulk
#                         if transaction_data['irctc_order_no'] and transaction_data['bank_refund_ref_no'] is not None:
#                             bulk_data_refunds.append(transaction_data)
#                             logger.info(f"Adding refund transaction: {transaction_data}")

#                     except KeyError as key_err:
#                         logger.error(f"Missing key in refund row {index} with data {row.to_dict()}: {str(key_err)}")
#                     except Exception as ex:
#                         logger.error(f"Unexpected error processing refund row {index} with data {row.to_dict()}: {str(ex)}")

#             # Insert booking and refund transactions into the database
#             logger.info(f"Prepared {len(bulk_data_bookings)} booking records for database insertion.")
#             logger.info(f"Prepared {len(bulk_data_refunds)} refund records for database insertion.")

#             if bulk_data_bookings:
#                 logger.debug("Bulk inserting booking transactions.")
#                 BookingTransaction.bulk_create_booking_transactions(bulk_data_bookings)  # Now passing a list of dicts
#                 logger.info(f"Processed and stored {len(bulk_data_bookings)} booking records from file: {file_name}")

#             if bulk_data_refunds:
#                 logger.debug("Bulk inserting refund transactions.")
#                 RefundTransaction.bulk_create_refund_transactions(bulk_data_refunds)  # Now passing a list of dicts
#                 logger.info(f"Processed and stored {len(bulk_data_refunds)} refund records from file: {file_name}")

#         else:
#             # If the transaction type is booking or refund only, process accordingly
#             if transaction_type == 'booking':
#                 expected_columns = [clean_column_name(col) for col in BANK_MAPPINGS[bank_name]['booking']['columns']]
#                 column_mapping = {clean_column_name(k): v for k, v in BANK_MAPPINGS[bank_name]['booking']['column_mapping'].items()}
#                 df = df.rename(columns=column_mapping)

#                 for index, row in df.iterrows():
#                     transaction_data = {'bank_code': BANK_CODE_MAPPING.get(bank_name)}
#                     try:
#                         transaction_data.update({
#                             'transaction_date': pd.to_datetime(row.get('transaction_date'), errors='coerce'),
#                             'credited_date': pd.to_datetime(row.get('credited_date'), errors='coerce'),
#                             'booking_amount': float(row.get('booking_amount')) if pd.notnull(row.get('booking_amount')) else 0.0,
#                             'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
#                             'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
#                         })
#                         logger.debug(f"Row {index}: Transaction data for booking: {transaction_data}")

#                         # Ensure transaction data is valid before adding to bulk
#                         if transaction_data['irctc_order_no'] and transaction_data['bank_booking_ref_no'] is not None:
#                             bulk_data_bookings.append(transaction_data)
#                             logger.info(f"Adding booking transaction: {transaction_data}")

#                     except KeyError as key_err:
#                         logger.error(f"Missing key in booking row {index} with data {row.to_dict()}: {str(key_err)}")
#                     except Exception as ex:
#                         logger.error(f"Unexpected error processing booking row {index} with data {row.to_dict()}: {str(ex)}")

#                 if bulk_data_bookings:
#                     logger.debug("Bulk inserting booking transactions.")
#                     BookingTransaction.bulk_create_booking_transactions(bulk_data_bookings)
#                     logger.info(f"Processed and stored {len(bulk_data_bookings)} booking records from file: {file_name}")

#             elif transaction_type == 'refund':
#                 expected_columns = [clean_column_name(col) for col in BANK_MAPPINGS[bank_name]['refund']['columns']]
#                 column_mapping = {clean_column_name(k): v for k, v in BANK_MAPPINGS[bank_name]['refund']['column_mapping'].items()}
#                 df = df.rename(columns=column_mapping)

#                 for index, row in df.iterrows():
#                     transaction_data = {'bank_code': BANK_CODE_MAPPING.get(bank_name)}
#                     try:
#                         transaction_data.update({
#                             'refund_date': pd.to_datetime(row.get('refund_date'), errors='coerce'),
#                             'bank_booking_ref_no': convert_to_int(row.get('bank_booking_ref_no')),
#                             'bank_refund_ref_no': convert_to_int(row.get('bank_refund_ref_no')),
#                             'refund_amount': float(row.get('refund_amount')) if pd.notnull(row.get('refund_amount')) else 0.0,
#                             'debited_date': pd.to_datetime(row.get('debited_date'), errors='coerce'),
#                             'irctc_order_no': convert_to_int(row.get('irctc_order_no')),
#                         })
#                         logger.debug(f"Row {index}: Transaction data for refund: {transaction_data}")

#                         # Ensure transaction data is valid before adding to bulk
#                         if transaction_data['irctc_order_no'] and transaction_data['bank_refund_ref_no'] is not None:
#                             bulk_data_refunds.append(transaction_data)
#                             logger.info(f"Adding refund transaction: {transaction_data}")

#                     except KeyError as key_err:
#                         logger.error(f"Missing key in refund row {index} with data {row.to_dict()}: {str(key_err)}")
#                     except Exception as ex:
#                         logger.error(f"Unexpected error processing refund row {index} with data {row.to_dict()}: {str(ex)}")

#                 # Bulk insert refund transactions into the database
#                 if bulk_data_refunds:
#                     logger.debug("Bulk inserting refund transactions.")
#                     RefundTransaction.bulk_create_refund_transactions(bulk_data_refunds)
#                     logger.info(f"Processed and stored {len(bulk_data_refunds)} refund records from file: {file_name}")

#         logger.info(f"File processing completed for {file_name}")

#     except Exception as e:
#         logger.error(f"Error processing file {file_name}: {e}")
#         raise  # Raise the exception for further handling/alerting


