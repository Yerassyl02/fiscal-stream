from sse_starlette.sse import EventSourceResponse
from datetime import datetime
from fastapi import FastAPI
from faker import Faker
import asyncio
import random
import json

app = FastAPI()
fake = Faker("ru_RU")

with open("products.json", "r", encoding="utf-8") as f:
    products_data = json.load(f)["products"]

def generate_dirty_fiscal_line():
    company_name = fake.company()

    # tax_payer_iin = fake.random_number(digits=12)
    # RAW ROW
    tax_payer_iin = random.choice([
        fake.random_number(digits=12), # CORRECT DATA
        fake.random_number(digits=11),   # короткий
        str(fake.random_number(digits=12)) + fake.lexify('?', letters='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') # с буквой
    ])

    kkt_factor_number = fake.bothify('?#########')
    kkt_reg_number = fake.random_number(digits=12)
    receipt_number = fake.random_int(min=1, max=999999)

    # date_time = datetime.now().strftime('%Y-%m-%d, %H:%M:%S')
    # дата может быть в разных форматах
    date_time = random.choice([
        datetime.now().strftime('%Y-%m-%d, %H:%M:%S'), # CORRECT DATA
        datetime.now().strftime('%d.%m.%y %H:%M'),
        datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    ])

    # orders
    items = []
    for _ in range(random.randint(1, 15)):
        category = random.choice(list(products_data.keys()))

        product = random.choice(products_data[category]) 
        # иногда делаем опечатки/символы
        if random.random() < 0.3:
            product += random.choice(["!!!", "***", "??", " "])

        price = round(random.uniform(100, 500000), 2)
        quantity = random.randint(1, 5)
        total =round(price * quantity, 2)

        # vat_rate = random.choice(['12%', '0%'])
        # vat_sum = round(total * (0.12 if vat_rate == '12%' else 0), 2)
        vat_rate = random.choice(['12%', '0%', '12%%', '', '8%0'])
        vat_sum = round(total * (0.12 if vat_rate.startswith('12') else 0), 2)

        items.append({
            'Category': category,
            'Name': product,
            'UnitPrice': price,
            'Quantity': quantity,
            'Total': total,
            'VAT': {
                'Rate': vat_rate,
                'Sum': vat_sum
            }
        })

    total_sum = sum(item['Total'] for item in items)
    payment = random.choice(['Cash', 'Card', 'QR'])

    fiscal_sign  = fake.random_number(digits=10)

    ofd_name = random.choice(["АО Казахтелеком", "АО Транстелеком"])
    ofd_site = 'https://ofd.telecom.kz' if ofd_name == 'АО Казахтелеком' else 'https://ofd.ttc.kz'
    ofd = {
        'Name': ofd_name,
        'Site': ofd_site
    }

    # address = f"{fake.city()}, {fake.street_address()}"
    address = random.choice([
        f"{fake.city()}, {fake.street_address()}",
        "unknown",
        ""
    ])

    # qr_code = fake.lexify(text="????????????????????????")
    qr_code = random.choice([
        fake.lexify(text="????????????????????????"),
        "",  # пустой
        "битый_qr_code"
    ])

    return (
        {
            'CompanyName': company_name,
            'TaxPayerIIN': tax_payer_iin,
            'KKTFactorNumber (ЗНМ)': kkt_factor_number,
            'KKTRegistrationNumber (РНМ)': kkt_reg_number,
            'ReceiptNumber': receipt_number,
            'DateTime': date_time,
            'Items': items,
            'TotalSum': total_sum,
            'Payment': payment,
            'FiscalSign (ФП)': fiscal_sign,
            'OFD': ofd,
            'Address': address,
            'QRCode': qr_code
        }
    )

@app.get("/stream")
async def stream():
    async def event_generator():
        while True:
            await asyncio.sleep(random.uniform(0.5, 2.5))  # рандомная задержка
            line = generate_dirty_fiscal_line()
            yield json.dumps(line, indent=4, ensure_ascii=False)

    return EventSourceResponse(event_generator())
