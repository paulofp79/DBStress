-- Version 3 of the swingbench simple order entry benchmark schema
-- Includes additional columns to some tables and a new table addresses
-- Focus on region_id as potential partition/sharding key

CREATE TABLE if not exists customers
(
    region_id                   NUMBER(4) constraint cust_region_nn not null,
    customer_id                 NUMBER(12) CONSTRAINT cust_custid_nn NOT NULL,
    cust_first_name             VARCHAR2(100) CONSTRAINT cust_fname_nn NOT NULL,
    cust_last_name              VARCHAR2(100) CONSTRAINT cust_lname_nn NOT NULL,
    customer_title              VARCHAR2(30),
    nls_language                VARCHAR2(3),
    nls_territory               VARCHAR2(90),
    credit_limit                NUMBER(9, 2),
    customer_email              VARCHAR2(100),
    customer_telephone_landline NUMBER(30),
    customer_telephone_mobile   NUMBER(30),
    total_purchases             NUMBER(6),
    total_spend                 NUMBER(9, 2),
    last_purchase_date          DATE,
    initial_registration        DATE,
    business                    BOOLEAN,
    company_name                VARCHAR2(100),
    account_mgr_id              NUMBER(12),
    customer_since              DATE,
    customer_class              VARCHAR(40),
    customer_interests          JSON,
    suggestions                 VARCHAR(40),
    date_of_birth               DATE,
    marketing_opt_in            BOOLEAN,
    partner_marketing_opt_in    BOOLEAN,
    is_active                   BOOLEAN,
    account_suspended           BOOLEAN,
    account_flagged             BOOLEAN,
    referrer_id                 NUMBER(12),
    preferred_address           NUMBER(12),
    preferred_card              NUMBER(12),
    last_updated                TIMESTAMP
)  &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists customer_relationships
(
    region_id                   NUMBER(4) constraint relationship_region_nn not null,
    source_customer_id          NUMBER(12) CONSTRAINT relationship_s_custid_nn NOT NULL,
    target_customer_id          NUMBER(12) CONSTRAINT relationship_t_custid_nn NOT NULL,
    description                 VARCHAR2(60) CONSTRAINT relationship_desc_nn NOT NULL
)  &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists addresses
(
    region_id                   NUMBER(4) constraint addresses_region_nn not null,
    address_id                  NUMBER(12) CONSTRAINT address_id_nn NOT NULL,
    customer_id                 NUMBER(12) CONSTRAINT address_cust_id_nn NOT NULL,
    date_created                DATE CONSTRAINT address_datec_nn NOT NULL,
    house_no_or_name            VARCHAR2(60),
    street_name                 VARCHAR2(60),
    town                        VARCHAR2(60),
    county                      VARCHAR2(60),
    country                     VARCHAR2(60),
    post_code                   VARCHAR(12),
    zip_code                    VARCHAR(12),
    residential                 BOOLEAN,
    shared_property             BOOLEAN,
    delivery_details            VARCHAR(200),
    last_updated                TIMESTAMP
)  &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists card_details
(
    region_id                   NUMBER(4) constraint card_region_nn not null,
    card_id                     NUMBER(12) CONSTRAINT card_id_nn NOT NULL,
    customer_id                 NUMBER(12) CONSTRAINT card_cust_id_nn NOT NULL,
    card_type                   VARCHAR2(60) CONSTRAINT card_type_nn NOT NULL,
    card_number                 NUMBER(12) CONSTRAINT card_number_nn NOT NULL,
    expiry_date                 DATE CONSTRAINT expiry_date_nn NOT NULL,
    is_valid                    VARCHAR2(30) CONSTRAINT is_valid_nn NOT NULL,
    security_code               NUMBER(6),
    last_updated                TIMESTAMP
)  &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists warehouses
(
    region_id                   NUMBER(4) constraint warehouses_region_nn not null,
    warehouse_id                NUMBER(6) constraint warehouses_warehouse_id_nn not null,
    warehouse_name              VARCHAR2(35) constraint warehouses_warehouse_name_nn not null,
    street_name                 VARCHAR2(60),
    town                        VARCHAR2(60),
    county                      VARCHAR2(60),
    country                     VARCHAR2(60),
    post_code                   VARCHAR(12)
);


CREATE TABLE if not exists order_items
(
    region_id                   NUMBER(4) constraint oi_region_nn not null,
    order_id                    NUMBER(12) CONSTRAINT oi_order_id_nn NOT NULL,
    line_item_id                NUMBER(3) CONSTRAINT oi_lineitem_id_nn NOT NULL,
    product_id                  NUMBER(6) CONSTRAINT oi_product_id_nn NOT NULL,
    unit_price                  NUMBER(8, 2),
    quantity                    NUMBER(8),
    dispatch_date               DATE,
    return_id                   NUMBER(12),
    gift_wrap                   VARCHAR(45),
    condition                   VARCHAR(45),
    supplier_id                 NUMBER(6),
    estimated_delivery          DATE
) &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists orders
(
    region_id                   NUMBER(4) constraint order_region_nn not null,
    order_id                    NUMBER(12) CONSTRAINT order_order_id_nn NOT NULL,
    order_date                  TIMESTAMP WITH LOCAL TIME ZONE CONSTRAINT order_date_nn NOT NULL,
    order_mode                  VARCHAR2(8),
    customer_id                 NUMBER(12) CONSTRAINT order_customer_id_nn NOT NULL,
    order_status                NUMBER(2),
    order_total                 NUMBER(10, 2),
    sales_rep_id                NUMBER(6),
    promotion_id                NUMBER(6),
    warehouse_id                NUMBER(6),
    delivery_type               VARCHAR(60),
    cost_of_delivery            NUMBER(6),
    wait_till_all_available     VARCHAR(60),
    delivery_address_id         NUMBER(12),
    customer_class              VARCHAR(60),
    card_id                     NUMBER(12),
    invoice_address_id          NUMBER(12),
    shipment_id                 NUMBER(12),
    return_id                   NUMBER(12),
    order_cancelled             BOOLEAN
)  &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);

create table if not exists suppliers
(
    region_id                   NUMBER(4) CONSTRAINT supplier_region_id_nn NOT NULL,
    supplier_id                 NUMBER(12) CONSTRAINT supplier_id_nn NOT NULL,
    supplier_name               VARCHAR2(100) CONSTRAINT supplier_name_nn NOT NULL,
    supplier_main_contact       VARCHAR2(100),
    head_office_address         VARCHAR2(200),
    street_name                 VARCHAR2(60),
    town                        VARCHAR2(60),
    county                      VARCHAR2(60),
    country                     VARCHAR2(60),
    telephone                   VARCHAR2(20),
    email                       VARCHAR2(100),
    active                      BOOLEAN
);

CREATE TABLE if not exists returns
(
    region_id                   NUMBER(4) constraint re_region_nn not null,
    return_id                   NUMBER(12) CONSTRAINT re_return_id_nn NOT NULL,
    order_id                    NUMBER(12) CONSTRAINT re_order_id_nn NOT NULL,
    line_item_id                NUMBER(3) CONSTRAINT re_lineitem_id_nn NOT NULL,
    return_date                 DATE CONSTRAINT re_re_date_nn NOT NULL,
    reason_for_return           VARCHAR2(30) CONSTRAINT re_reason_nn NOT NULL,
    condition_of_return         VARCHAR2(30) CONSTRAINT re_condition_nn NOT NULL,
    refunded                    BOOLEAN,
    replacement                 BOOLEAN
) &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists shipments
(
    region_id                   NUMBER(4) constraint sh_region_nn not null,
    shippment_id                NUMBER(12) CONSTRAINT sh_return_id_nn NOT NULL,
    order_id                    NUMBER(12) CONSTRAINT sh_order_id_nn NOT NULL,
    line_item_id                NUMBER(3) CONSTRAINT sh_lineitem_id_nn NOT NULL,
    shipment_date               DATE CONSTRAINT sh_ship_date_nn NOT NULL,
    type_of_shipment            VARCHAR2(20) CONSTRAINT sh_ship_type CHECK (type_of_shipment in ('next day', 'express', 'standard', 'economy')),
    carrier                     VARCHAR2(100)
) &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists inventories
(
    region_id                   NUMBER(4) CONSTRAINT inventory_region_id_nn NOT NULL,
    product_id                  NUMBER(6) CONSTRAINT inventory_prooduct_id_nn NOT NULL,
    warehouse_id                NUMBER(6) CONSTRAINT inventory_warehouse_id_nn NOT NULL,
    warehouse_zone              NUMBER(8),
    warehouse_ailse             NUMBER(8),
    warehouse_shelf             NUMBER(4),
    warehouse_shelf_section     NUMBER(4),
    quantity_on_hand            NUMBER(8) CONSTRAINT inventory_qoh_nn NOT NULL,
    reorder_threshold           NUMBER(8),
    last_refreshed              DATE,
    next_expected_delivery      DATE
) &compress initrans 16 pctfree 90 pctused 5;


CREATE TABLE if not exists product_information
(
    region_id                   NUMBER(4) CONSTRAINT product_region_id_nn NOT NULL,
    product_id                  NUMBER(6) CONSTRAINT product_product_id_nn NOT NULL,
    product_name                VARCHAR2(50) CONSTRAINT product_product_name_nn NOT NULL,
    product_description         VARCHAR2(2000),
    category_id                 NUMBER(4) CONSTRAINT product_category_id_nn NOT NULL,
    weight_class                NUMBER(1),
    warranty_period             INTERVAL YEAR TO MONTH,
    supplier_id                 NUMBER(6),
    product_status              VARCHAR2(20),
    list_price                  NUMBER(8, 2),
    min_price                   NUMBER(8, 2),
    catalog_url                 VARCHAR2(50),
    CONSTRAINT product_status_lov
        CHECK (product_status in ('orderable', 'planned', 'under development', 'obsolete')
            )
) ;

CREATE TABLE if not exists logon
(
    region_id                   NUMBER(4),
    logon_id                    NUMBER CONSTRAINT logon_logon_id_nn NOT NULL,
    customer_id                 NUMBER CONSTRAINT logon_customer_id_nn NOT NULL,
    logon_time                  TIMESTAMP
) &compress initrans 16 STORAGE (INITIAL 8M NEXT 8M);


CREATE TABLE if not exists product_descriptions
(
    region_id                   NUMBER(4) CONSTRAINT product_desc_region_id_nn NOT NULL,
    product_id                  NUMBER(6),
    language_id                 VARCHAR2(3),
    translated_name             NVARCHAR2(50) CONSTRAINT translated_name_nn NOT NULL,
    translated_description      NVARCHAR2(2000) CONSTRAINT translated_desc_nn NOT NULL
) ;

create table if not exists regions
(
    region_id                   NUMBER(4) CONSTRAINT region_id_nn NOT NULL,
    region_name                 VARCHAR2(100) CONSTRAINT region_name_nn NOT NULL,
    region_head                 VARCHAR2(100),
    house_no_or_name            VARCHAR2(60),
    street_name                 VARCHAR2(60),
    town                        VARCHAR2(60),
    county                      VARCHAR2(60),
    country                     VARCHAR2(60),
    telephone                   VARCHAR2(20)
);

CREATE TABLE if not exists orderentry_metadata
(
    metadata_key                VARCHAR2(30),
    metadata_value              VARCHAR2(30)
);


-- End;


