# dbt Airbnb Case

基于 [dbt](https://www.getdbt.com/) + Databricks 构建的 Airbnb 业务数据分层项目，演示了从原始数据（Source）到分析宽表（Mart）的完整数据建模流程，涵盖 **增量加载、Snapshot 历史追踪、自定义宏、数据测试** 等核心特性。

---

## 📐 数据分层架构

项目采用经典的 **Bronze / Silver / Gold** 三层架构（对应 dbt 的 `staging` / `intermediate` / `mart`）：

| 层级 | 目录 | Schema | 物化方式 | 说明 |
|------|------|--------|----------|------|
| Bronze（原始层） | `models/staging` | `bronze` | `incremental` | 对源数据做类型转换、字段标准化，按 `ingested_at` 增量加载 |
| Silver（清洗层） | `models/intermediate` | `silver` | `incremental` + `merge` | 业务清洗、衍生字段计算、打标签 |
| Gold（应用层） | `models/mart` | `gold` | `view` / `table` | 面向分析的事实表、宽表（One Big Table） |

```
source(airbnb_source) ─▶ staging (bronze) ─▶ intermediate (silver) ─▶ mart (gold)
```

---

## 📁 项目结构

```
dbt_airbnb_case/
├── analyses/              # 临时分析 SQL（不会被物化）
├── macros/                # 自定义宏
│   ├── clean_string.sql       # 字符串清洗（trim + 大写）
│   ├── multiply.sql           # 数值相乘并四舍五入
│   ├── schema.sql              # 自定义 schema 生成规则
│   └── tag.sql                  # 数值分档打标签
├── models/
│   ├── staging/            # Bronze 层
│   │   ├── source.yml          # 数据源定义
│   │   ├── stg_bookings.sql
│   │   ├── stg_hosts.sql
│   │   └── stg_listings.sql
│   ├── intermediate/       # Silver 层
│   │   ├── silver_bookings.sql
│   │   ├── silver_hosts.sql
│   │   └── silver_listings.sql
│   └── mart/                # Gold 层
│       ├── gold_fact_bookings.sql
│       └── obt.sql              # One Big Table 宽表
├── snapshots/               # SCD2 历史快照
│   ├── dim_hosts_snapshot.sql
│   └── dim_listings_snapshot.sql
├── tests/                    # 自定义数据测试
│   └── source_test.sql
├── dbt_project.yml
└── packages.yml
```

---

## 🗄️ 数据源（Source）

数据源定义在 `models/staging/source.yml`：

| Source | Database | Schema | Tables |
|--------|----------|--------|--------|
| `airbnb_source` | `dbt_airbnb` | `source` | `listings`, `bookings`, `hosts` |

---

## 🧱 模型说明

### Staging（Bronze）

对源表做字段类型转换、统一命名，并基于 `ingested_at` 实现增量加载。

| 模型 | 主键 | 说明 |
|------|------|------|
| `stg_listings` | `listing_id` | 房源基础信息（房型、城市、价格等） |
| `stg_hosts` | `host_id` | 房东信息（注册时间、是否超级房东、回复率） |
| `stg_bookings` | `booking_id` | 预订记录（入住天数、金额、状态） |

### Intermediate（Silver）

在 Staging 基础上做业务清洗与衍生字段计算：

| 模型 | 核心逻辑 |
|------|----------|
| `silver_bookings` | 使用 `multiply_and_round` 宏计算 `total_amount`（房费 × 入住天数） |
| `silver_hosts` | 清洗 `host_name`（空格替换为下划线），按 `response_rate` 打标签（`very_good` / `good` / `fair` / `poor`） |
| `silver_listings` | 使用 `tag_col` 宏对 `price_per_night` 分档打标签（`low` / `medium` / `high`） |

### Mart（Gold）

| 模型 | 物化方式 | 说明 |
|------|----------|------|
| `gold_fact_bookings` | `view` | 预订事实表，关联房源城市、房型等维度信息 |
| `obt` | `table` | One Big Table，整合 Bookings、Listings、Hosts 三张表的全部字段，供 BI / 分析直接使用 |

---

## 🛠️ 自定义宏（Macros）

| 宏 | 功能 |
|----|------|
| `clean_string(column_name)` | 对字符串字段先 `TRIM` 再转大写 |
| `multiply_and_round(col1, col2, precision=2)` | 两数相乘并按指定精度四舍五入 |
| `tag_col(col)` | 根据数值区间打标签：`<100` → `low`，`<200` → `medium`，其余 → `high` |
| `generate_schema_name(custom_schema_name, node)` | 覆盖 dbt 默认 schema 命名规则，直接使用模型配置的 `schema`，不拼接 target schema 前缀 |

---

## 📸 快照（Snapshots）

用于追踪维度数据的历史变更（SCD Type 2），存储在 `snapshots` schema：

| Snapshot | 来源模型 | 策略 | 监控字段 |
|----------|----------|------|----------|
| `hosts_snapshot` | `stg_hosts` | `check` | `host_name`, `is_superhost`, `response_rate` |
| `listings_snapshot` | `stg_listings` | `check` | `price_per_night`, `room_type`, `bedrooms`, `bathrooms`, `accommodates`, `city`, `country` |

> ⚠️ 注意：`dbt_project.yml` 中 snapshots 配置的默认策略为 `timestamp`（基于 `updated_at`/`ingested_at`），但两个 snapshot 文件中均显式指定为 `check` 策略，文件内配置会覆盖项目级默认配置。

---

## ✅ 数据测试

`tests/source_test.sql`：自定义单测，检查 `airbnb_source.bookings` 中是否存在 `booking_amount < 200` 的异常记录，配置 `severity = warn`，发现问题时仅告警不阻断 `dbt build`。

可结合 `dbt_utils` 包（已在 `packages.yml` 中声明，版本 `1.3.3`）为各模型补充通用测试（如 `unique`、`not_null`、`relationships` 等）。

---

## 🚀 快速开始

### 1. 环境准备

本项目使用 [uv](https://github.com/astral-sh/uv) 管理 Python 依赖，技术栈为 dbt + Databricks：

```bash
# 安装依赖（dbt-core、dbt-databricks）
uv sync
```

依赖版本要求（见 `pyproject.toml`）：
- Python `>= 3.12`
- `dbt-core >= 1.11.6`
- `dbt-databricks >= 1.11.6`

### 2. 配置 Profile

在 `~/.dbt/profiles.yml` 中配置 Databricks 连接信息，`profile` 名称需与 `dbt_project.yml` 中的 `profile: dbt_airbnb_case` 一致：

```yaml
dbt_airbnb_case:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: dbt_airbnb
      schema: dev_<your_name>
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <your-databricks-token>
      threads: 4
```

### 3. 安装依赖包

```bash
dbt deps
```

### 4. 运行项目

```bash
# 运行全部模型
dbt run

# 运行数据测试
dbt test

# 生成快照
dbt snapshot

# 一次性执行 run + test + snapshot
dbt build
```

### 5. 调试 / 探索

`analyses/` 目录中提供了一些探索性 SQL（如 `explore.sql`、`ifElse.sql`、`loop.sql` 及各模型的测试脚本），可通过 `dbt compile` 编译后查看生成的 SQL。

---

## 📦 包依赖

| 包 | 版本 |
|----|------|
| [dbt-labs/dbt_utils](https://github.com/dbt-labs/dbt-utils) | `1.3.3` |

---

## 📚 参考资料

- [dbt 官方文档](https://docs.getdbt.com/docs/introduction)
- [dbt-databricks 适配器文档](https://docs.getdbt.com/reference/warehouse-setups/databricks-setup)
- [dbt_utils 包文档](https://github.com/dbt-labs/dbt-utils)
