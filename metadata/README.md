https://dev.mysql.com/doc/refman/5.7/en/information-schema.html

Abstract:
INFORMATION_SCHEMA is a database within each MySQL instance, the place that stores information about all the other databases that the MySQL server maintains.
The INFORMATION_SCHEMA database contains several read-only tables. They are actually views, not base tables

Advantages of SELECT syntax:
- It conforms to Codd's rules, because all access is done on tables. (https://en.wikipedia.org/wiki/Codd's_12_rules#Rules)
- You can use the familiar syntax of the SELECT statement, and only need to learn some table and column names.
- You can filter, sort, concatenate, and transform the results from INFORMATION_SCHEMA queries into whatever format your application needs.


SCHEMATA: Store for Databases
https://dev.mysql.com/doc/refman/5.7/en/schemata-table.html
https://dev.mysql.com/doc/refman/5.7/en/show-databases.html

SELECT { * | schema_name AS `Database` }
  FROM information_schema.schemata For Desktchema.schemata
  [WHERE schema_name LIKE 'wild']

SHOW { databases | schemas }
  [LIKE 'wild']

cols:
    catalog_name                (def)
    schema_name                 ('schema_name', name of the DB)
    default_character_set_name  (charset...)
    default_collation_name      (charset...)
    sql_path                    (-)

EXAMPLES:
    SELECT catalog_name, schema_name, default_character_set_name, default_collation_name, sql_path FROM SCHEMATA;
    SELECT schema_name FROM SCHEMATA where schema_name="gitql";



TABLES: Store for Tables
https://dev.mysql.com/doc/refman/5.7/en/tables-table.html
https://dev.mysql.com/doc/refman/5.7/en/show-tables.html

SELECT { * | table_name }
  FROM information_schema.tables
  WHERE table_schema = 'schema_name'
  [AND table_name LIKE 'wild']

SHOW tables
    [{FROM | IN} schema_name]
    [LIKE 'wild']

cols:
    table_catalog   (def)
    table_schema    ('schema_name', name of the DB)
    table_name      ('table_name', name of the table)
    table_type      (BASE TABLE or VIEW)
    engine          (?)
    version         (num)
    row_format      ( Dynamic | ?)
    table_rows      (NULL in schemata, estimation in other)
    avg_row_length  (num)
    data_length     (num)
    max_data_length (num)
    index_length    (num)
    data_free       (-)
    auto_increment  ( num | - )
    create_time     (time)
    update_time     (time)
    check_time      (-)
    table_collation (charset...)
    checksum        (-)
    create_options  (-)
    table_comment   (-)

EXAMPLES
    SELECT table_catalog, table_schema, table_name, table_type, engine, version, table_rows, create_time, update_time, table_collation, table_comment FROM TABLES;
    SELECT table_name FROM TABLES WHERE table_schema="gitql";



COLUMNS: Store for Columns
https://dev.mysql.com/doc/refman/5.7/en/columns-table.html
https://dev.mysql.com/doc/refman/5.7/en/show-columns.html

SELECT { * | column_name, data_type, is_nullable, column_key, column_default, extra }
  FROM information_schema.columns
  WHERE table_name = 'table_name'
  [AND table_schema = 'schema_name']
  [AND column_name LIKE 'wild']

SHOW columns
    {FROM | IN} table_name
    [{FROM | IN} schema_name]
    [LIKE 'wild']

cols:
    table_catalog           (def)
    table_schema            ('schema_name', name of the DB)
    table_name              ('table_name', name of the table)
    column_name	            ('field_name', name of the field)
    ordinal_position        (sequence)
    column_default          ( - | value)
    is_nullable             ( YES | NO )
    data_type               (Type)
    character_maximum_length (?)
    character_octet_length  (?)
    numeric_precision       (-)
    numeric_scale           (-)
    datetime_precision      (-)
    character_set_name      (charset..., or NULL if not string)
    collation_name          (charset..., or NULL if not string)
    column_type             (Type)
    column_key              ( - | PRI | UNI | MUL )
    extra                   ( - | AUTO_INCREMENT | TIMESTAMP | DATETIME)
    privileges              (-)
    column_comment          (string)
    generation_expression   (-)

EXAMPLES
    SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_set_name, collation_name, column_key, extra, column_comment FROM COLUMNS;
    SELECT column_name, data_type, is_nullable, column_key, column_default, extra FROM COLUMNS WHERE table_schema="gitql" and table_name="commits";
    SELECT column_name, data_type, is_nullable, column_key, column_default, extra FROM COLUMNS WHERE table_name="commits";