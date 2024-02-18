insert into ads.ads_user_tag
with label %s
select COALESCE(t1.tyc_user_id, t2.tyc_user_id) as tyc_user_id,
       vip_unpay_time                           as vip_unpay_time,
       svip_pay_last_time                       as svip_pay_last_time,
       vip_pay_last_time                        as vip_pay_last_time,
       svip_unpay_time                          as svip_unpay_time,
       first_show_vip_paypoint_time             as first_show_vip_paypoint_time,
       now()                                    as update_time
from (SELECT tyc_user_id,
             MAX(CASE
                     WHEN sku_id >= 50 AND sku_id <= 70 AND order_status NOT IN ('1', '-10')
                         THEN create_date END) AS vip_unpay_time,
             MAX(CASE
                     WHEN sku_id >= 71 AND sku_id <= 73 AND order_status = 1 AND actual_amount > 0
                         THEN create_date END) AS svip_pay_last_time,
             MAX(CASE
                     WHEN sku_id >= 50 AND sku_id <= 70 AND order_status = 1 AND actual_amount > 0
                         THEN create_date END) AS vip_pay_last_time,
             MAX(CASE
                     WHEN sku_id >= 71 AND sku_id <= 73 AND order_status NOT IN ('1', '-10')
                         THEN create_date END) AS svip_unpay_time
      FROM dwd.dwd_order_info
      WHERE (sku_id >= 50
                 AND sku_id <= 70
          OR sku_id >= 71
                 AND sku_id <= 73)
        AND update_time >= TIMESTAMPADD(MINUTE, -2, CURRENT_TIMESTAMP())
        and update_time <= CURRENT_TIMESTAMP()
        and tyc_user_id is not null
      GROUP BY tyc_user_id) t1 full join
(
  select
    tyc_user_id,
    min(request_time) as first_show_vip_paypoint_time
  from
    dwd.dwd_pay_point_com_detail
  where
    sensor_event in ('Show', 'VIP_show')
    and tyc_user_id is not null
     AND update_time >= TIMESTAMPADD(MINUTE, -2, CURRENT_TIMESTAMP()) and update_time <= CURRENT_TIMESTAMP()
    GROUP by
      tyc_user_id
) t2
on
    t1.tyc_user_id = t2.tyc_user_id