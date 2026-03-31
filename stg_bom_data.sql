with source as (
    select * from {{ source('raw', 'bom_data') }}
),

staged as (
    select
        SALES_ORDER_ID,
        LINE_ITEM_SEQ_NUM,
        BOM_SEQ_NUM,
        ITEM_ID,
        ITEM_NAME,
        RAW_MATERIAL_ITEM_ID,
        RAW_MATERIAL_NAME,
        RAW_MATERIAL_QUANTITY,
        RAW_MATERIAL_GROUP,
        UNIT_COST,
        RAW_MATERIAL_QUANTITY * UNIT_COST   as extended_cost,
        HIERARCHICAL_LEVEL,
        SCRAP_PERCENT
    from source
    where RAW_MATERIAL_ITEM_ID != -1
)

select * from staged