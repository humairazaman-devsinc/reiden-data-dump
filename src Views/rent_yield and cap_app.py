from database import DatabaseManager


class LocationTransformer:
    def __init__(self, batch_size: int = 5000):
        self.db = DatabaseManager()
        self.batch_size = batch_size

    def transform_location_data(self) -> int:
        print('transform')
        update_query = """
        CREATE OR REPLACE VIEW vw_property_performance AS
        WITH sales AS (
            SELECT
                location_id,
                property_type,
                no_of_bedrooms,
                currency,
                measurement,
                year,
                month,
                date_period,
                average_unit_price AS avg_sales_price
            FROM transactions_avg
            WHERE activity_type = 'sales'
        ),
        rents AS (
            SELECT
                location_id,
                property_type,
                no_of_bedrooms,
                currency,
                measurement,
                year,
                month,
                date_period,
                average_unit_price AS avg_rent_price
            FROM transactions_avg
            WHERE activity_type = 'rent'
        )
        SELECT
            s.location_id,
            s.property_type,
            s.no_of_bedrooms,
            s.currency,
            s.measurement,
            s.year,
            s.month,
            s.date_period,
            s.avg_sales_price,
            r.avg_rent_price,
            ROUND((r.avg_rent_price * 12 / NULLIF(s.avg_sales_price,0)) * 100, 2) AS rental_yield_pct,
            ROUND(((s.avg_sales_price - LAG(s.avg_sales_price) OVER (PARTITION BY s.location_id, s.property_type, s.no_of_bedrooms ORDER BY s.year, s.month))
                    / NULLIF(LAG(s.avg_sales_price) OVER (PARTITION BY s.location_id, s.property_type, s.no_of_bedrooms ORDER BY s.year, s.month),0)) * 100, 2) AS capital_appreciation_pct
        FROM sales s
        LEFT JOIN rents r
        ON s.location_id = r.location_id
        AND s.property_type = r.property_type
        AND s.no_of_bedrooms = r.no_of_bedrooms
        AND s.currency = r.currency
        AND s.measurement = r.measurement
        AND s.date_period = r.date_period;

        """
        print('transform')
        return self.db.transform_data(update_query)


def main():
    print('main')
    transformer = LocationTransformer()
    transformer.transform_location_data()


if __name__ == "__main__":
    main()
