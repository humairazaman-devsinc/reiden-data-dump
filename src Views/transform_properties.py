class PropertyTransformer:
    def __init__(self, batch_size: int = 5000):
        self.db = DatabaseManager()
        self.batch_size = batch_size

    def transform_properties(self):
        query = """
        SELECT p.id AS property_id,
              p.name,
              p.name_local,
              p.description,
              p.description_local,
              pd.level->>'id' AS level_id,
              pd.level->>'name' AS level_name,
              pd.status->>'id' AS status_id,
              pd.status->>'name' AS status_name,
              pd.types->>'id' AS type_id,
              pd.units AS units_json,
              pd.developer_prices AS dev_prices_json,
              pd.attributes AS attributes_json,
              pd.primary_image,
              pd.parties,
              pd.location_id,
              pd.gla,
              pd.office_gla,
              pd.typical_gla_floor,
              pd.built_up_area,
              pd.building_height,
              pd.land_area,
              pd.created_at,
              pd.updated_on
        FROM property p
        JOIN property_details pd ON p.id = pd.property_id;
        """
        results = self.db.run_query(query)
        levels = set()
        statuses = set()
        types = set()
        dev_prices = set()
        units = set()
        attributes = set()

        for row in results:
            property_id = row["property_id"]

            # Collect unique levels
            if row["level_id"]:
                levels.add((row["level_id"], row["level_name"]))

            # Collect unique statuses
            if row["status_id"]:
                statuses.add((row["status_id"], row["status_name"]))

            # Collect unique types
            if row["type_id"]:
                types.add((row["type_id"], row["type_name"]))

            # Collect developer prices
            dev_prices.update((
                (property_id, dp.get('quarter_date'), dp.get('price_size'), dp.get('price_size_min'), dp.get('price_size_max'))
                for dp in row["dev_prices_json"] or []
            ))

            # Collect units
            units.update((
                (property_id, u.get('unit_count'), u.get('bedroom_count'), u.get('size'), u.get('size_min'), u.get('size_max'), row['created_at'], row['updated_on'])
                for u in row["units_json"] or []
            ))

            # Collect attributes
            for attr in row["attributes_json"] or []:
                group_id = attr.get("group_id")
                attr_id = attr.get("id")
                if group_id:
                    attributes.add((group_id, attr.get('group_name')))
                value_id = attr.get("value_object_id")
                if value_id:
                    attributes.add((value_id, attr.get('label'), attr.get('label_local')))
                attributes.add((attr_id, attr.get('name'), group_id, value_id))

    def bulk_insert(self, table, columns, data):
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        self.db.bulk_insert(sql, data)
        print("Property transformation completed.")
