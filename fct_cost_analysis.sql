with sales as (
    select * from {{ ref('stg_sales_orders') }}
),

bom as (
    select
        SALES_ORDER_ID,
        sum(extended_cost)          as total_bom_cost,
        count(distinct ITEM_ID)     as total_components,
        sum(RAW_MATERIAL_QUANTITY)  as total_material_qty
    from {{ ref('stg_bom_data') }}
    group by SALES_ORDER_ID
),

joined as (
    select
        s.SALES_ORDER_ID,
        s.SALES_ORDER_NUM,
        s.order_date,
        s.customer_name,
        s.SALESPERSON_ID,
        s.DIVISION_ID,
        s.sale_amount,
        s.material_cost,
        s.material_cost_pct,
        s.cost_target_status,
        s.TOTAL_PIECES,
        s.PROJECT_NAME,
        b.total_bom_cost,
        b.total_components,
        b.total_material_qty,
        s.material_cost - b.total_bom_cost  as cost_variance
    from sales s
    left join bom b on s.SALES_ORDER_ID = b.SALES_ORDER_ID
)

select * from joined