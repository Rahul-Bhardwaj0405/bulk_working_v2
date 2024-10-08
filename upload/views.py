# Create your views here.
from django.shortcuts import render, redirect
from django.http import JsonResponse
from .forms import UploadFileForm
from .tasks import process_uploaded_files
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def upload_files(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            bank_name = form.cleaned_data['bank_name']
            transaction_type = form.cleaned_data['transaction_type']
            files = request.FILES.getlist('file')
            
            # Prepare a list to hold the file contents
            file_contents = []
            file_format = None  # Track file format

            for file in files:
                if file.size == 0:  # Check for empty files
                    logger.error(f"File {file.name} is empty.")
                    return JsonResponse({'message': f'File {file.name} is empty.'}, status=400)

                file_content = file.read()
                file_name = file.name

                logger.debug(f"Processing file: {file_name} with size {file.size} bytes.")

                # Determine file format based on extension
                if file_name.endswith('.xlsx'):
                    file_format = 'excel'
                elif file_name.endswith('.csv'):
                    file_format = 'csv'
                else:
                    logger.error(f"Unsupported file extension for file: {file_name}")
                    return JsonResponse({'message': 'Unsupported file extension. Only .xlsx and .csv files are allowed.'}, status=400)

                file_contents.append(file_content)

            # Ensure that file_format is set
            if file_format is None:
                logger.error("Could not determine file format for uploaded files.")
                return JsonResponse({'message': 'Unable to determine file format. Please upload .xlsx or .csv files.'}, status=400)

            # Pass the full list of file contents, bank name, transaction type, and format to the task
            process_uploaded_files.delay(file_contents, bank_name, transaction_type, file_format)

            return JsonResponse({'message': 'Files uploaded successfully. Processing started.'}, status=202)
    else:
        form = UploadFileForm()
    
    return render(request, 'upload.html', {'form': form})


# def transaction_list(request):
#     transactions = TransactionData.objects.all().order_by('-date')
#     paginator = Paginator(transactions, 20)  # Show 20 transactions per page
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)
    
#     total_amount = TransactionData.objects.aggregate(Sum('amount'))['amount__sum']
    
#     context = {
#         'page_obj': page_obj,
#         'total_amount': total_amount,
#     }
#     return render(request, 'transaction_list.html', context)








# import oracledb

# # Establish a connection to the production Oracle database
# with oracledb.connect(user='PGACT7', password='Oct2024', dsn='10.78.14.42:1725/rptdb_srv.cris.org.in') as connection:
#     with connection.cursor() as cursor:
#         # Use FETCH FIRST to limit the rows (Oracle SQL syntax)
#         sql = """SELECT "PAYMENT_DATE", "ENTITY_ID", "AMOUNT", "BANK_ID" 
#                  FROM TRANSACTION_DB.ET_PAYMENT_CASH 
#                  WHERE "BANK_ID" = 40 AND ROWNUM <= 2"""

#         # Execute the query
#         cursor.execute(sql)
#         # Fetch and print the results
#         for row in cursor:
#             print(row)