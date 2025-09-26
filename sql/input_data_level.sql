with sku_info as (
select 
	sku_nm,
	sku_uuid,
	fragile_flg, 
	coalesce(storage_condition_code, 'none') as storage_condition_code,
	coalesce(sku_category_level_1_nm,'none') as sku_category_level_1_nm,
	coalesce(sku_category_level_2_nm,'none') as sku_category_level_2_nm,
	coalesce(sku_category_level_3_nm,'none') as sku_category_level_3_nm,
	coalesce(sku_category_level_4_nm,'none') as sku_category_level_4_nm
from grp_em.dim_sku
where 1=1
	and valid_to_dt = '5999-01-01'
	and sku_category_level_1_nm = 'Безалкогольные напитки'
),

-- ПОСТАВКИ ТОЛЬКО С ВЫБРАННЫМИ SKU 
receipts as (
select receipt_id
from grp_sandbox.imoshev_wms_fmcg_inbnd_rows im
group by 1
having count(*) = sum
	(
    case when product_id in (select sku_uuid from sku_info) 
    then 1 else 0 end
    )
), 

-- ВСЯ НУЖНАЯ ИНФОРМАЦИЯ ПО ПОСТАВКАМ-SKU
receipts_info as (
select 
	im.warehouse_name,
	im.receipt_id,
	sku_nm,
	sku_uuid,
	fragile_flg,
	storage_condition_code,
	sku_category_level_1_nm,
	sku_category_level_2_nm,
	sku_category_level_3_nm,
	sku_category_level_4_nm, 
	inbnd_type, 				
	supplier_id,				-- поставщик
	sku_weight_kg,				-- вес одной штуки
	sku_volume_litr,			-- объем одной штуки
	sku_type,					-- весовой или штучный
	accepted_quantity,														-- принятые кг (в модели не участвует)
	accepted_volume_litr,													-- принятый объем
	started_at,																-- старт приемки
	extract(hour from started_at)::text as start_hour,						-- час приемки
	extract(day from started_at)::text as start_day,						-- день приемки
	extract(week from started_at)::text as start_week,						-- неделя приемки
	extract(dow from started_at)::text as start_weekday,					-- день недели приемки
	inbnd_duration_min
from grp_sandbox.imoshev_wms_fmcg_inbnd_rows im
	join receipts r on r.receipt_id = im.receipt_id
	join sku_info si on si.sku_uuid = im.product_id
where 1=1 
 order by im.receipt_id
 ),
 
 -- ДЕЛИМ ВРЕМЯ ПРИЕМКИ ПРОПОРЦИОНАЛЬНО КОЛИЧЕСТВО ПРИНЯТЫХ SKU 
 parts as (
 select 
 	t.receipt_id,
 	t.inbnd_duration_min,
 	sku_category_level_4_nm,
 	t2.s as all_sku_cnt,
 	sum(t.accepted_quantity) as sku_category_cnt
 from receipts_info t
 	join ( 
 		select 
 			receipt_id,
 			sum(accepted_quantity) s
 		from receipts_info
 		group by 1
 		) t2 on t2.receipt_id = t.receipt_id
 group by t.receipt_id, 
 	t.inbnd_duration_min, 
 	t.sku_category_level_4_nm, 
 	t2.s
 ),
 
 -- ДАННЫЕ ДЛЯ МОДЕЛИ ПО ПОСТАВКЕ
 total_receipt_info as (
 select 
 	distinct 
 	receipt_id,
 	extract(hour from started_at)::text as start_hour,						-- час приемки
	extract(day from started_at)::text as start_day,						-- день приемки
	extract(week from started_at)::text as start_week,						-- неделя приемки
	extract(dow from started_at)::text as start_weekday,
	warehouse_name,
	supplier_id,
	inbnd_type
 from grp_sandbox.imoshev_wms_fmcg_inbnd_rows
 )
 
 select 
    ri.*,
 	sku_category_level_4_nm,
 	sku_category_cnt,
 	(1.0 * sku_category_cnt / all_sku_cnt) * inbnd_duration_min as inbnd_duration_min
 from parts p
 	join total_receipt_info ri on ri.receipt_id = p.receipt_id