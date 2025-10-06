from database import DatabaseManager


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
                p.internal_status_id,
                point(
                    (p.loc_point->'coordinates'->>0)::float,
                    (p.loc_point->'coordinates'->>1)::float
                ) AS loc_point,
                p.main_type_name,
                p.main_subtype_name,
                p.created_on AS api_created_on,
                p.updated_on AS api_updated_on,
                p.import_date,
                p.import_type,
                pd.level->>'name' AS property_level,
                pd.status->>'name' AS property_status,
                pd.types->>'name' AS property_type,
                pd.parent_id,
                pd.location_id,
                pd.units AS units_json,
                pd.developer_prices AS dev_prices_json,
                pd.attributes AS attributes_json,
                pd.primary_image,
                pd.images AS images_json,
                pd.parties AS parties_json,
                pd.search_terms,
                pd.dld_status,
                pd.elapsed_time_status,
                pd.gla,
                pd.office_gla,
                pd.typical_gla_floor,
                pd.built_up_area,
                pd.building_height,
                pd.land_area,
                pd.created_at,
                pd.updated_on
        FROM property p
        JOIN property_details pd ON p.id = pd.property_id
        LIMIT 10;
        """
        results = self.db.run_query(query)

        properties = []
        dev_prices = []
        units = []
        attributes = []

        for row in results:
            # property
            properties.append(
                {
                    "property_id": row.get["property_id"],
                    "name": row.get["name"],
                    "name_local": row.get["name_local"],
                    "description": row.get["description"],
                    "description_local": row.get["description_local"],
                    "internal_status_id": row.get["internal_status_id"],
                    "loc_point": row.get["loc_point"],
                    "property_level": row.get["property_level"],
                    "property_status": row.get["property_status"],
                    "property_type": row.get["property_type"],
                    "main_type_name": row.get["main_type_name"],
                    "main_subtype_name": row.get["main_subtype_name"],
                    "parent_id": row.get["parent_id"],
                    "location_id": row.get["location_id"],
                    "created_on": row.get["api_created_on"],
                    "updated_on": row.get["api_updated_on"],
                    "import_date": row.get["import_date"],
                    "import_type": row["import_type"],
                }
            )
            

            # developer prices
            for dp in row["dev_prices_json"] or []:
                dev_prices.append(
                    {
                        "property_id": row.get["property_id"],
                        "quarter_date": dp.get("quarter_date"),
                        "price_size_aed_int": dp.get("price_size_aed_int"),
                        "price_size_min_usd_int": dp.get("price_size_min_usd_int"),
                        "price_size_max_aed_int": dp.get("price_size_max_aed_int"),
                    }
                )

            # units
            for u_group in row["units_json"] or []:
                for u in u_group.get("units", []):
                    units.append(
                        {
                            "property_id": row.get["property_id"],
                            "unit_count": u.get("number_of_unit"),
                            "bedroom_count": u.get("number_of_bedroom"),
                            "size": u.get("size"),
                            "size_min": u.get("size_min"),
                            "size_max": u.get("size_max"),
                            "created_at": row["created_at"],
                            "updated_on": row["updated_on"],
                        }
                    )

            # attributes
            for attr in row["attributes_json"] or []:
                attributes.append(
                    {
                        "property_id": row.get["property_id"],
                        "name": attr.get("name"),
                        "label_local": attr.get("label_local"),
                        "label": attr.get("label"),
                        "type": attr.get("type"),
                        "group": attr.get("group", {}).get("name"),
                        "value_array": attr.get("value_array"),
                        "value_object": attr.get("value_object"),
                        "value_bool": attr.get("value_bool"),
                        "value_text": attr.get("value_text"),
                        "value_date": attr.get("value_date"),
                    }
                )

        print("p:", properties)
        # bulk insert
        self.bulk_insert("cleaned_property", properties)
        self.bulk_insert("property_developer_prices", dev_prices)
        self.bulk_insert("property_units", units)
        self.bulk_insert("property_attributes", attributes)

        print("Property transformation completed.")

    def bulk_insert(self, table, data):
        print("inserting into: ", table)
        if not data:
            print("no data")
            return
        # infer columns from dict keys
        columns = data[0].keys()
        print("columns: ", columns)
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        print("sql", sql)
        values = [tuple(d[col] for col in columns) for d in data]
        print("values", values)
        try:
            self.db.bulk_insert(sql, values)
        except Exception as e:
            print("error:", e)

def main():
    print('main')
    transformer = PropertyTransformer()
    transformer.transform_properties()


if __name__ == "__main__":
    main()
