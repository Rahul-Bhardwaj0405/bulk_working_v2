# Generated by Django 5.1.1 on 2024-10-07 05:47

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='BookingTransaction',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('bank_code', models.IntegerField()),
                ('transaction_date', models.DateField(blank=True, null=True)),
                ('credited_date', models.DateField(blank=True, null=True)),
                ('irctc_order_no', models.BigIntegerField(blank=True, null=True, unique=True)),
                ('bank_booking_ref_no', models.BigIntegerField(blank=True, null=True, unique=True)),
                ('booking_amount', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'indexes': [models.Index(fields=['bank_code'], name='upload_book_bank_co_f5ca5a_idx'), models.Index(fields=['irctc_order_no', 'bank_booking_ref_no'], name='upload_book_irctc_o_2932bc_idx')],
            },
        ),
        migrations.CreateModel(
            name='RefundTransaction',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('bank_code', models.IntegerField()),
                ('refund_date', models.DateField(blank=True, null=True)),
                ('bank_booking_ref_no', models.BigIntegerField(blank=True, null=True, unique=True)),
                ('bank_refund_ref_no', models.BigIntegerField(blank=True, null=True, unique=True)),
                ('refund_amount', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('debited_date', models.DateField(blank=True, null=True)),
                ('irctc_order_no', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'indexes': [models.Index(fields=['bank_code'], name='upload_refu_bank_co_533707_idx'), models.Index(fields=['irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no'], name='upload_refu_irctc_o_d64b32_idx')],
            },
        ),
    ]
