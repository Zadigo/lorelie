# Lorelie

A module that creates a simple interface ORM for SQLITE for any Python application.

```python
from krytpone.tables import Table
from krytpone.fields import CharField

table = Table('my_table', database_name='my_database', inline_build=True, fields=[
    CharField('name')
])
table.prepare()
```

The above will create a single table called `my_table` in the `my_database` sqlite database and with the a column called `name`.

By default, when calling the `Table` instance, a connection will is not automatically established with sqlite. You need to set the `inline_build` to True and then call `prepare` to run the table creation sequence.

In order to manage multiple tables in a database, using `Database` implements additional functionnalities such as migrations (or table state tracking).

```python
from krytpone.tables import Table
from krytpone.fields import CharField

table = Table('my_table', database_name='my_database', fields=[
    CharField('name')
])

database = Database('my_database', tables=[table])
database.makemigrations()
dataase.migrate()
table = database.get_table('my_table')
```

## Migrations

```json
{
    "id": null,
    "date": null,
    "number": 0,
    "indexes": [],
    "tables": [
        {
            "name": "celebrities",
            "fields": [
                {
                    "name": "name",
                    "params": [
                        "name",
                        "varchar(300)",
                        "not null",
                    ]
                }
            ],
            "indexes": {}
        }
    ]
}
```


## Functions

### Lower

The Lower function is designed to facilitate text manipulation within your SQLite database by converting each value of a specified column to lowercase. This function is particularly useful for standardizing text data, enabling efficient comparison, sorting, and search operations.

```python
database.objects.annotate('table_name', new_column_name=Lower('column_name'))
```

```sql
SELECT LOWER(column_name) AS new_column_name FROM table_name;
```

__parameters__

* table_name (str): The name of the table where the annotation will be applied.
* new_column_name (str): The desired name for the new column containing the lowercase values.
* column_name (str): The name of the column whose values will be converted to lowercase.

__Example__

Consider a scenario where you have a table named 'employees' with a column named 'full_name', which contains names in various formats (e.g., "John DOE", "Mary Smith", "alice@example.com"). You may want to standardize these names to lowercase for consistency and ease of comparison. This can be achieved using the Lower function as follows:


### Upper

The Upper function facilitates text manipulation within your SQLite database by converting each value of a specified column to uppercase. This function is particularly useful for standardizing text data, enabling efficient comparison, sorting, and search operations.

```python
database.objects.annotate('table_name', new_column_name=Upper('column_name'))
```

```sql
SELECT UPPER(column_name) AS new_column_name FROM table_name;
```

__parameters__

* table_name (str): The name of the table where the annotation will be applied.
* new_column_name (str): The desired name for the new column containing the lowercase values.
* column_name (str): The name of the column whose values will be converted to lowercase.

__Example__

Consider a scenario where you have a table named 'employees' with a column named 'full_name', which contains names in various formats (e.g., "John DOE", "Mary Smith", "alice@example.com"). You may want to standardize these names to lowercase for consistency and ease of comparison. This can be achieved using the Lower function as follows:

### Length

The Upper function facilitates text manipulation within your SQLite database by converting each value of a specified column to uppercase. This function is particularly useful for standardizing text data, enabling efficient comparison, sorting, and search operations.

```python
database.objects.annotate('table_name', new_column_name=Length('column_name'))
```

```sql
SELECT LEN(column_name) AS new_column_name FROM table_name;
```

__parameters__

* table_name (str): The name of the table where the annotation will be applied.
* new_column_name (str): The desired name for the new column containing the lowercase values.
* column_name (str): The name of the column whose values will be converted to lowercase.

__Example__

Consider a scenario where you have a table named 'articles' with a column named 'content', which contains textual content of varying lengths. You may want to analyze the distribution of article lengths or filter articles based on their length. This can be achieved using the Length function as follows:

### ExtractYear

The `ExtractYear` function extracts the year component from a date value in a specified column.

```python
db.objects.annotate('table_name', year=ExtractYear('date_column'))
```

```sql
SELECT STRFTIME('%Y', date_column) AS year FROM table_name;
```

__Example__

This will add a new column named 'year' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.


### ExtractMonth

Like [ExtractYear](#extractyear) but extracts the month component from a date value in a specified column.

```python
db.objects.annotate('table_name', year=ExtractMonth('date_column'))
```

```sql
SELECT STRFTIME('%m', date_column) AS year FROM table_name;
```

__Example__

This will add a new column named 'month' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.

### ExtractDay

Like [ExtractYear](#extractyear) and [ExtractYear](#extractmonth) but extracts the day component from a date value in a specified column.

```python
db.objects.annotate('table_name', year=ExtractDay('date_column'))
```

```sql
SELECT STRFTIME('%d', date_column) AS year FROM table_name;
```

__Example__

This will add a new column named 'day' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.

### ExtractHour

Like [ExtractYear](#extractyear), [ExtractYear](#extractmonth) and [ExtractYear](#extractday) but extracts the hour component from a datetime value in a specified column.

```python
db.objects.annotate('table_name', year=ExtractHour('date_column'))
```

```sql
SELECT STRFTIME('%H', date_column) AS year FROM table_name;
```

__Example__

This will add a new column named 'hour' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.

### ExtractMinute

Like [ExtractYear](#extracthour) but extracts the minute component from a datetime value in a specified column.

```python
db.objects.annotate('table_name', year=ExtractMinute('date_column'))
```

```sql
SELECT STRFTIME('%M', date_column) AS year FROM table_name;
```

__Example__

This will add a new column named 'minute' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.

### MD5Hash

The `MD5Hash` function calculates the MD5 hash of each value in a specified column.

```python
db.objects.annotate('table_name', year=MD5Hash('date_column'))
```

```sql
SELECT hash(name) AS hash_name FROM table_name;
```

__Example__

This will add a new column named 'minute' to the 'celebrities' table, containing the year component of each value from the 'date_of_birth' column.
