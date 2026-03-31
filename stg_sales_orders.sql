with source as (
    select * from {{ source('raw', 'sales_orders') }}
),

staged as (
    select
        SALES_ORDER_ID,
        SALES_ORDER_NUM,
        ORDER_DATE::date                    as order_date,
        CUSTOMER_ID,
        BILL_TO_COMPANY_NAME                as customer_name,
        SALESPERSON_ID,
        DIVISION_ID,
        SAL_ORDER_STATUS_ID                 as order_status_id,
        COST                                as material_cost,
        AMOUNT                              as sale_amount,
        PROFIT,
        PROFIT_PERCENT                      as profit_pct,
        TOTAL_PIECES,
        PROJECT_NAME,
        COST / NULLIF(AMOUNT, 0)            as material_cost_pct,
        case
            when COST / NULLIF(AMOUNT, 0) > 0.40 then 'Over Target'
            else 'Within Target'
        end                                 as cost_target_status
    from source
)

select * from staged