from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv(override=True)

engine = create_engine(os.getenv("CLICKHOUSE_URL"))

def get_total_product():
    with engine.connect() as connection:
        result = connection.execute(text("select count(*) from productDim;"))
        total_product = result.scalar()
    return total_product


# total_product = get_total_product()
# print(type(total_product), total_product)


def get_total_brand():
    with engine.connect() as connection:
        result = connection.execute(text("select count(*) from brandDim;"))
        total_brand = result.scalar()
    return total_brand

# total_brand = get_total_brand()
# print(type(total_brand), total_brand)

def get_total_seller():
    with engine.connect() as connection:
        result = connection.execute(text("select count(*) from sellerDim;"))
        total_seller = result.scalar()
    return total_seller

# total_seller = get_total_seller()
# print(type(total_seller), total_seller)

def get_top_n_total_product_by_cateogry(n: int):
    with engine.connect() as connection:
        result = connection.execute(text(f"select top {n} category, count(*) AS Count from productDim group by category order by Count desc;"))
        total_product_by_category = result.fetchall()
    return pd.DataFrame(total_product_by_category, columns=["Category", "Count"])

# print(get_total_product_by_cateogry())


def get_top_n_potential_product(n: int):
    with engine.connect() as connection:
        result = connection.execute(
            text(f"""select top {n} productDim.title, ss.sold, ss.sellerCnt
                    from
                        (select *
                        from productSnapshotFact as prdSnap1
                        join
                            (select urlId, count(sellerId) as sellerCnt
                            from productSnapshotFact as prdSnap2
                            group by prdSnap2.urlId) as sellerCntByUrlId
                        on sellerCntByUrlId.urlId = prdSnap1.urlId) as ss
                    join productDim
                    on ss.urlId = productDim.urlId
                    order by ss.sellerCnt asc, ss.sold desc;""")
            )
        potential_products = result.fetchall()
    return pd.DataFrame(potential_products, columns=["Product", "Sold", "Seller Count"])

# print(get_potential_product())

def get_top_n_sold_brand(n: int):
    with engine.connect() as connection:
        result = connection.execute(
            text(f"""select top {n} brandDim.brand, sum(sold) as sold
                    from productSnapshotFact as prdSnap
                    join brandDim
                    on prdSnap.brandId = brandDim.brandId
                    group by brandDim.brand
                    order by sold desc""")
        )
        top_n_sold_brand = result.fetchall()
    return pd.DataFrame(top_n_sold_brand, columns=["Brand", "Sold"])

# print(get_top_n_sold_brand(10))

def get_top_n_sold_seller(n: int):
    with engine.connect() as connection:
        result = connection.execute(
            text(f"""select top {n} sellerDim.seller, sum (sold) as sold
                    from productSnapshotFact as prdSnap
                    join sellerDim
                    on prdSnap.sellerId = sellerDim.sellerId
                    group by sellerDim.seller
                    order by sold desc""")
        )
        top_n_sold_seller = result.fetchall()
    return pd.DataFrame(top_n_sold_seller, columns=["Seller", "Sold"])

# print(get_top_n_sold_seller(10))