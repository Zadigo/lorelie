# Lorelie

Welcome to Lorelie, a lightweight framework designed to simplify database interactions and queries using SQLite. Lorelie makes it easy to define tables, fields, and perform database migrations. This guide will walk you through the process of creating a database and performing queries using the Lorelie framework.

## Database

The `Database` class in Lorelie is the main wrapper responsible for creating and organizing tables, indexes, constraints, and other database structures. It serves as the central point for managing database schema and executing migrations, ensuring that your database stays in sync with your application models. By using the `Database` class, you can easily define and manipulate the core components of your database, facilitating efficient and structured data management.

__Setting Up Your Database__

```python
from lorelie.database.base import Database
from lorelie.fields.base import CharField
from lorelie.tables import Table

# Define a table with the required fields
table = Table('celebrities', fields=[
    CharField('name')
])

# Initialize the database with the defined table
db = Database(table)

# Run the migration to create the table in the SQLite database
db.migrate()
```

By default, when creating a `Table` instance, a connection to the SQLite database is not automatically established. The connection and table creation are handled by the `migrate` method of the Database class.

The Database class is the main wrapper for creating and organizing tables, indexes, constraints, and other database objects. It manages multiple tables in a database and provides additional functionalities such as migrations, which ensure that your database schema is synchronized with your application models.

The migrate method establishes the connection to the database, creates the tables, indexes, and applies any defined constraints.

__Migrations__

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


## Fields

The `Field` class defines a generic field in a database table. It serves as a base class for more specific field types like `CharField`. It includes various attributes and methods to manage the properties and behavior of database fields.

__Conversion methods__

```python
def to_python(self, data):
    return data
```
Converts the given data to a Python string. If the data is None, it returns None.

```python
def to_database(self, data):
    return data
```

Converts the given data to a string if it is not already one, then calls the superclass method to convert it to the database representation.

__Usage example__

```python

from lorelie.database.base import Database
from lorelie.fields.base import CharField, IntegerField
from lorelie.tables import Table

table = Table(
    'products',
    fields=[
        CharField('name'),
        IntegerField('price', default=0),
        DateTimeField('created_on', auto_add=True)
    ]
)

db = Database(table)
db.migrate()
```

### CharField

The `CharField` class is a subclass of Field designed to handle text data.

__parameters__

* `name` (str): The name of the field.
* `max_length` (int, optional): The maximum length for the field value (applicable for text fields).
* `null` (bool, optional): Whether the field can have null values. Defaults to False.
* `primary_key` (bool, optional): Whether the field is a primary key. Defaults to False.
* `default` (optional): The default value for the field.
* `unique` (bool, optional): Whether the field value must be unique. Defaults to False.
* `validators` (list, optional): A list of validators to apply to the field value.

### IntegerField

The `IntegerField` class is a subclass of `Field` designed to handle integer data.

### FloatField

The `FloatField` class is a subclass of `Field` designed to handle floating-point data.

__Parameters__

* `name` (str): The name of the field.
* `min_value` (int, optional): The minimum value for the field.
* `max_value` (int, optional): The maximum value for the field.

### JSONField

The `JSONField` class is a subclass of `Field` designed to handle JSON data.

### BooleanField

The `BooleanField` class is a subclass of `Field` designed to handle boolean data.

### AutoField

The `AutoField` class is a subclass of `IntegerField` designed to handle auto-incrementing primary key fields.

### DateField

The `DateField` class is a subclass of `Field` designed to handle date data.

__Parameters__

* `name` (str): The name of the field.
* `auto_update` (bool, optional): Automatically update the field with the current date on each update. Defaults to False.
* `auto_add` (bool, optional): Automatically set the field to the current date on creation. Defaults to False.

### DateTimeField

The `DateTimeField` class is a subclass of `DateField` designed to handle datetime data.

### TimeField

The `TimeField` is a subclass of `DateTimeField` designed to handle time data.

### EmailField

The `EmailField` is a subclass of `CharField` designed to handle email data.

### FilePathField

The `FilePathField` is a subclass of `CharField` designed to handle file path data.

### SlugField

The `SlugField` is a subclass of `CharField` designed to handle slug data.

### UUIDField

The `UUIDField` is a subclass of `CharField` designed to handle UUID data.

### URLField

The `URLField` is a subclass of `CharField` designed to handle url data.

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


## Aggregation

This section covers several classes that implement various aggregating functions used in an SQLite database context. These functions are useful for performing operations like counting rows, calculating averages, and determining statistical measures directly within the database or in a Python environment.

__Properties and Methods__

* `aggregate_name`: Returns the name of the aggregate function based on the field name.
* `python_aggregation`(values): Should be implemented by subclasses to define the local aggregation logic.
* `use_queryset`(field, queryset): Aggregates values locally using a queryset.
* `as_sql`(backend): Generates the SQL representation of the function.

### Count

The `Count` class is used to count the number of rows that match a specified condition or all rows in a table if no condition is specified.

```python
db.objects.aggregate('products', Count('price'))
```

### Avg

The `Avg` class calculates the average of the specified field.

```python
db.objects.aggregate('products', Avg('price'))
```

### Variance

The `Variance` class calculates the variance of the specified field.

```python
db.objects.aggregate('products', Avg('price'))
```

### StDev

The `StDev` class calculates the standard deviation of the specified field.

```python
db.objects.aggregate('products', StDev('price'))
```

### Sum

The `Sum` class calculates the sum of the specified field.

```python
db.objects.aggregate('products', Sum('price'))
```

### MeanAbsoluteDifference

The `MeanAbsoluteDifference` class calculates the mean absolute difference of the specified field.

```python
db.objects.aggregate('products', MeanAbsoluteDifference('price'))
```

### MeanAbsoluteDifference

The `MeanAbsoluteDifference` class calculates the mean coefficient of variation of the specified field.

```python
db.objects.aggregate('products', MeanAbsoluteDifference('price'))
```

### CoefficientOfVariation

The `CoefficientOfVariation` class calculates the coefficient of variation of the specified field.

```python
db.objects.aggregate('products', CoefficientOfVariation('price'))
```

### Max

The `Max` class returns the maximum value of the specified field.

```python
db.objects.aggregate('products', Max('price'))
```

### Min

The `Min` class returns the minimum value of the specified field.

```python
db.objects.aggregate('products', Min('price'))
```
