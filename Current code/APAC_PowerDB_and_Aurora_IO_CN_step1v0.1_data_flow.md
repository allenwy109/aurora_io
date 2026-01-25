# APAC_PowerDB_and_Aurora_IO_CN_step1v0.1.py 数据流总结

本文件总结脚本在“中国路径”下的输入来源与输出去向，便于评估所需权限。
说明：脚本里标注为“OBSOLETE/NOT IN USE”的区块如果没有注释掉，仍会执行。

## 运行上下文（按代码写死）
- Country: `China`
- 源 SQL Server: `ANVDEVSQLVPM01`, 数据库 `WM_POWER_RENEWABLES`
- 目标 SQL Server: `ANVDEVSQLVPM01`, 数据库 `Aurora_APAC_DEV_China`

## 输入（读取）

### 文件 / Excel
- `L:\Power_Renewables\Inputs\APAC_Transmission_China.xlsx`（工作表：`LiveUpdate`, `T&D Tariffs`）
- `RawWind&SolarData.xlsx`
- `APAC_NewBuilds_Central China.xlsm`
- `APAC_Hydro.xlsx`
- `APAC_Assumptions.xlsx`（PlantLife, VOM, HeatRate, CapacityFactor, FixedCost, EmissionRate, EmissionPrice, StorageDuration）
- `APAC_Fuels.xlsx`
- `negativebalancing_soln.xlsx`

可选（代码中已注释）：
- `APAC_DistributedCapacity.xlsx`（分布式容量）
- `APAC_Transmission.xlsx`（旧版传输数据）
- `APAC_Assumptions.xlsx` 的 `HourlyShape`（EV 形状）

### 源 SQL Server（WM_POWER_RENEWABLES）
通过 `pd.read_sql_query` 读取（含函数内部调用）：
- `vAID_Topology_Zones`
- `vAPAC_Plant_Attributes_Annual_LIVE`
- `vAPAC_LoadDurationCurve_Normalised_Forecast_LIVE`
- `vAPAC_Inflation_YoY_Latest`
- `vAPAC_Transmission_LIVE`
- `APAC_AID_HydroVectors_LIVE`
- `vAPAC_Shapes_Monthly_LatestYear`
- `vAPAC_Shapes_8760_to_168_LatestYear_ModelZone`
- `vAPAC_Plant_Fuel_Price_Annual_LIVE`
- `vAPAC_PowerProjects_LIVE`
- `vAPAC_PowerProjects_NewBuild_LIVE`
- `vAPAC_WindProjectList_LIVE`
- `vAPAC_Plant_Fuel_MinMax_Annual_LIVE`
- `vAPAC_PowerCapacity_BalancingQty_LIVE`（平衡容量读入，用于聚合时的容量校准）

（仅中国路径：不包含 Vietnam / Thailand 等国家的特殊读取逻辑）

### 目标 SQL Server（Aurora_APAC_DEV_China）
用于回读或克隆：
- `tbl_AID_Fuel`
- `tbl_AID_Resources`
- `tbl_AID_Time_Series_Annual`
- `tbl_AID_Operating_Rules`

## 输出（写入）

### 源 SQL Server（WM_POWER_RENEWABLES）
写入方式：`upload_sql`、`reload_tbl`（truncate + insert）、`update_sqltbl_by`、以及直接 `MERGE/UPDATE` SQL。

来自 Excel 或转换后的数据：
- `APAC_PowerTransmission_LIVE`（append）
- `APAC_PowerTransmission_Tariffs_LIVE`（append）
- `APAC_PowerTransmission_InfrastructureProject_LIVE`（append）
- `APAC_PowerTransmission_Datasets`（append）
- `APAC_PowerTransmission_Tariffs_Datasets`（append）
- `APAC_PowerTransmission_InfrastructureProject_Datasets`（append）
- `APAC_Shapes_Raw8760_LIVE`（truncate + append）
- `APAC_PowerProjects_NewBuild_LIVE`（按 Zone 更新）
- `APAC_PowerProjects_NewBuild_Datasets`（append）
- `APAC_Shapes_Monthly_LIVE`（truncate + append）
- `APAC_AID_HydroVectors_LIVE`（truncate + append）
- `APAC_PlantAttribute_AnnualAssumptions_LIVE`（truncate + append）
- `APAC_PlantFuel_Annual_LIVE`（truncate + append）

平衡容量修正：
- 输入来源：`vAPAC_PowerCapacity_BalancingQty_LIVE`（SQL 视图）
- 人工修正来源：`negativebalancing_soln.xlsx`（BalancingEdits 工作表）
- `tmp_Balancing`（创建/替换）
- `APAC_PowerProjects_Balancing_LIVE`（MERGE 更新/插入）
- `tmp_Balancing`（删除）
- `APAC_PowerProjects_Balancing_LIVE`（多条 UPDATE）

可选（代码中已注释，且与中国路径无关）：
- `APAC_DistributedCapacity_LIVE`（reload）
- `APAC_DistributedCapacity_Datasets`（append）
- `APAC_Shapes_Hourly_LIVE`（reload）

### 目标 SQL Server（Aurora_APAC_DEV_China）
写入方式：`reload_tbl`（truncate + insert）、`update_aid_id`（按 ID delete + insert）、`upload_sql`（append）。

核心 AID 表：
- `tbl_AID_Topology_Zones`（reload）
- `tbl_AID_Topology_Areas`（reload）
- `tbl_AID_Demand_Hourly_Shapes`（reload）
- `tbl_AID_Demand_Monthly_Peak`（reload）
- `tbl_AID_Demand_Monthly`（reload）
- `tbl_AID_Transmission_Links`（reload）
- `tbl_AID_Time_Series_Annual`（多处更新：通胀、传输费、装机容量、燃料价格、燃料上限、储能上限、运行规则）
- `tbl_AID_Hydro_Vectors`（reload）
- `tbl_AID_Hydro_Monthly`（reload）
- `tbl_AID_Time_Series_Weekly`（reload）
- `tbl_AID_Time_Series_Monthly`（reload）
- `tbl_AID_Resources`（reload + update/append）
- `tbl_AID_Emission_Prices`（reload）
- `tbl_AID_Emission_Rates`（reload）
- `tbl_AID_Fuel`（reload + append）
- `tbl_AID_Resource_Groups`（reload）
- `tbl_AID_Storage`（reload）
- `tbl_AID_Constraint`（reload）

运行规则克隆：
- `tbl_AID_Operating_Rules`（append）
- `tbl_AID_Time_Series_Annual`（append）

## 权限影响（概览，China 路径）
- 源库：需要对上述视图/表的 SELECT 权限；对写入目标的 INSERT/UPDATE/DELETE/TRUNCATE 权限；`tmp_Balancing` 需要 CREATE/DROP。
- 目标库：对回读表的 SELECT；对写入目标的 INSERT/DELETE/TRUNCATE（`update_aid_id` 会先 DELETE）。
- 文件：需要读取上述 Excel 文件权限（包含 `L:\` 网络盘路径与本地工作目录中的 `negativebalancing_soln.xlsx`）。
