-- Add columns and populate with computed availability data
WITH property_units AS (
    SELECT
        p.id as property_id,
        SUM((unit->>'number_of_unit')::int) AS total_units
    FROM property p
    CROSS JOIN LATERAL jsonb_array_elements(p.units) AS unit
    WHERE units IS NOT NULL
    GROUP BY p.id
),
sold_units AS (
    SELECT property_id, COUNT(*) AS sold_units
    FROM transaction_sales
    WHERE transaction_type IN ('Sales - Off-Plan', 'Sales - Ready')
    GROUP BY property_id
),
rented_units AS (
    SELECT prop_property_id as property_id, COUNT(*) AS rented_units
    FROM transaction_raw_rent
    WHERE end_date IS NULL OR end_date > CURRENT_DATE
    GROUP BY prop_property_id
),
availability_data AS (
    SELECT
        pu.property_id,
        COALESCE(pu.total_units - su.sold_units - ru.rented_units, pu.total_units) AS available_units,
        CASE 
            WHEN pu.total_units > 0 THEN (COALESCE(pu.total_units - su.sold_units - ru.rented_units, pu.total_units)::decimal / pu.total_units)
            ELSE 0
        END AS available_unit_pct
    FROM property_units pu
    LEFT JOIN sold_units su ON pu.property_id = su.property_id
    LEFT JOIN rented_units ru ON pu.property_id = ru.property_id
)
-- First add the columns
ALTER TABLE property ADD COLUMN IF NOT EXISTS available_units INTEGER DEFAULT 0;
ALTER TABLE property ADD COLUMN IF NOT EXISTS available_unit_pct DECIMAL(5,4) DEFAULT 0.0000;

-- Then update with computed data
UPDATE property 
SET 
    available_units = ad.available_units,
    available_unit_pct = ad.available_unit_pct
FROM availability_data ad
WHERE property.id = ad.property_id;