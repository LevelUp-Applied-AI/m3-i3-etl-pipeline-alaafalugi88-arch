from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    tables = ["customers", "products", "orders", "order_items"]
    data = {}

    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"Extracted {table}: {len(df)} rows")
        data[table] = df

    return data


def transform(data_dict):
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # 🔥 مهم: طباعة الأعمدة عشان نتأكد
    print("Customers columns:", customers.columns)

    df = orders.merge(order_items, on="order_id")
    df = df.merge(products, on="product_id")

    df["line_total"] = df["quantity"] * df["unit_price"]

    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    print(f"After filtering: {len(df)} rows")

    # Aggregation
    summary = df.groupby("customer_id").agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    ).reset_index()

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    # Top category
    category_rev = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_category = category_rev.sort_values(["customer_id", "line_total"], ascending=[True, False])
    top_category = top_category.drop_duplicates("customer_id")[["customer_id", "category"]]
    top_category = top_category.rename(columns={"category": "top_category"})

    # 🔥 الحل النهائي (بدون تخمين اسم العمود)
    name_col = None
    for col in customers.columns:
        if "name" in col:
            name_col = col
            break

    if name_col is None:
        raise ValueError("No name column found in customers table!")

    summary = summary.merge(customers[["customer_id", name_col]], on="customer_id")
    summary = summary.rename(columns={name_col: "customer_name"})

    summary = summary.merge(top_category, on="customer_id")

    print(f"Transformed: {len(summary)} customers")

    return summary


def validate(df):
    results = {}

    results["no_nulls"] = not df["customer_id"].isnull().any() and not df["customer_name"].isnull().any()
    results["positive_revenue"] = (df["total_revenue"] > 0).all()
    results["no_duplicates"] = not df["customer_id"].duplicated().any()
    results["positive_orders"] = (df["total_orders"] > 0).all()

    for k, v in results.items():
        print(f"{k}: {'PASS' if v else 'FAIL'}")

    if not all(results.values()):
        raise ValueError("Validation failed!")

    return results


def load(df, engine, csv_path):
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows to DB and CSV")


def main():
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/amman_market"

    engine = create_engine(DATABASE_URL)

    print("Starting ETL pipeline...")

    data = extract(engine)
    df = transform(data)
    validate(df)
    load(df, engine, "output/customer_analytics.csv")

    print("ETL completed successfully!")


if __name__ == "__main__":
    main()