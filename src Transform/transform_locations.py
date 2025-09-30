from database import DatabaseManager


class LocationTransformer:
    def __init__(self, batch_size: int = 5000):
        self.db = DatabaseManager()
        self.batch_size = batch_size

    def transform_location_data(self) -> int:
        print('transform')
        update_query = """
        INSERT INTO public.cleaned_location (
            location_id,
            location_name,
            city_id,
            county_id,
            district_id,
            country_code,
            description,
            geo_point,
            photo_path,
            created_at,
            updated_at
        )
        SELECT
            location_id,
            location_name,
            city_id,
            county_id,
            district_id,
            country_code,
            description,
            POINT(
                (geo_point->>'lat')::float,
                (geo_point->>'lon')::float
            ),
            photo_path,
            created_at,
            CURRENT_TIMESTAMP
        FROM public.location;
        """
        print('transform')
        return self.db.transform_data(update_query)


def main():
    print('main')
    transformer = LocationTransformer()
    transformer.transform_location_data()


if __name__ == "__main__":
    main()
