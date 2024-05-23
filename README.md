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

## Tables

A database table is a structured collection of data organized in rows and columns, similar to a spreadsheet. Each table in a database is designed to store specific types of information, and it typically consists of one or more fields (or columns) that define the nature of the data stored.

Here's an example breakdown:

```python
from lorelie.constraints import CheckConstraint, UniqueConstraint
from lorelie.database.indexes import Index
from lorelie.expressions import Q
from lorelie.fields.base import CharField
from lorelie.tables import Table

table = Table(
    'celebrities',
    fields=[
        CharField('name')
    ],
    constraints=[
        UniqueConstraint('name', fields=['name']),
        CheckConstraint('age', Q(age__gt=22))
    ],
    index=[
        Index('idx_name', fields=['name'])
    ],
    str_field='name',
    ordering=['name']
)

```

### Fields

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

#### CharField

The `CharField` class is a subclass of Field designed to handle text data.

__parameters__

* `name` (str): The name of the field.
* `max_length` (int, optional): The maximum length for the field value (applicable for text fields).
* `null` (bool, optional): Whether the field can have null values. Defaults to False.
* `primary_key` (bool, optional): Whether the field is a primary key. Defaults to False.
* `default` (optional): The default value for the field.
* `unique` (bool, optional): Whether the field value must be unique. Defaults to False.
* `validators` (list, optional): A list of validators to apply to the field value.

#### IntegerField

The `IntegerField` class is a subclass of `Field` designed to handle integer data.

#### FloatField

The `FloatField` class is a subclass of `Field` designed to handle floating-point data.

__Parameters__

* `name` (str): The name of the field.
* `min_value` (int, optional): The minimum value for the field.
* `max_value` (int, optional): The maximum value for the field.

#### JSONField

The `JSONField` class is a subclass of `Field` designed to handle JSON data.

#### BooleanField

The `BooleanField` class is a subclass of `Field` designed to handle boolean data.

#### AutoField

The `AutoField` class is a subclass of `IntegerField` designed to handle auto-incrementing primary key fields.

#### DateField

The `DateField` class is a subclass of `Field` designed to handle date data.

__Parameters__

* `name` (str): The name of the field.
* `auto_update` (bool, optional): Automatically update the field with the current date on each update. Defaults to False.
* `auto_add` (bool, optional): Automatically set the field to the current date on creation. Defaults to False.

#### DateTimeField

The `DateTimeField` class is a subclass of `DateField` designed to handle datetime data.

#### TimeField

The `TimeField` is a subclass of `DateTimeField` designed to handle time data.

#### EmailField

The `EmailField` is a subclass of `CharField` designed to handle email data.

#### FilePathField

The `FilePathField` is a subclass of `CharField` designed to handle file path data.

#### SlugField

The `SlugField` is a subclass of `CharField` designed to handle slug data.

#### UUIDField

The `UUIDField` is a subclass of `CharField` designed to handle UUID data.

#### URLField

The `URLField` is a subclass of `CharField` designed to handle url data.

### Indexes

An index is a database object designed to improve the performance of queries by enabling faster retrieval of rows. It works similarly to an index in a book, which allows you to find information quickly without reading every page. By creating an index on specific columns of a database table, the database can locate and access the data more efficiently, particularly in large datasets.

* __Enhanced Query Performance:__ Indexes allow the database to find rows more quickly and efficiently, which can significantly speed up search operations, sorting, and filtering
* __Improved Data Retrieval:__ Indexes reduce the amount of data the database engine needs to scan, thus improving the speed of data retrieval operations
* __Optimized Sorting and Filtering:__ Indexes enable faster execution of queries that involve sorting (ORDER BY) and filtering (WHERE clauses)

In Lorelie, indexes can be created in two ways:

__Global Index on a Column__

The first method involves creating a global index on a specific column. This index enhances the retrieval of rows by using pointers, allowing the database to find data more efficiently. For instance, consider the following code snippet:

```python
Index('idx_age', fields=['age'])
```

Here, an index named `idx_age` is created on the `age` column of a table. This index improves the performance of queries involving the 'age' column by enabling faster data retrieval operations.

__Conditional Index__

The second method involves creating a conditional index based on specific criteria. This type of index is tailored to optimize performance for queries that meet certain conditions. For example:

```python
Index('idx_age', fields=['age'], condition=Q(age__gt=25))
```

In this case, an index named `idx_age` is created on the `age` column of a table, but it's conditioned to include only rows where the age is greater than 25. This conditional index ensures that the database engine efficiently retrieves data matching the specified condition, enhancing query performance for relevant queries.

### Constraints

__Unique Constraints__

You can enforce unique constraints in your database to ensure that the values within specific columns or combinations of columns are unique across the table. A unique constraint guarantees that no two rows in the table can have the same value(s) in the specified column(s).

```python
UniqueConstraint('name', fields=['name'])
```

In this example, a unique constraint named `name` is created on the `name` column of the table. This constraint ensures that no two rows in the table can have the same value in the 'name' column, thereby enforcing uniqueness for each name entry.

__Check Constraints__

You can apply check constraints to your database tables to enforce specific conditions or rules on the values within certain columns. A check constraint ensures that the values inserted or updated in the specified column(s) meet certain criteria defined by you.

```python
CheckConstraint('age', Q(age__gt=22))
```

In this example, a check constraint is applied to the `age` column of the table, specifying that the age value must be greater than 22. This constraint ensures that only records with an age greater than 22 can be inserted or updated in the database, enforcing the specified condition on the age column.

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


## Expressions

Expressions in Lorelie serve as powerful tools for constructing SQL queries and representing various components of database operations. These expressions enable users to define conditions, select specific fields, filter data, handle logical operations, and manipulate values within their database queries.

The main expressions are:

* __Q:__ Represents query conditions and filters, allowing users to construct complex filter expressions to retrieve specific data from the database.
* __F:__ Represents database fields or columns, providing a convenient way to reference and manipulate column values within SQL queries.
* __Case:__ Represents conditional expressions in SQL queries, enabling users to perform conditional logic and transformations on data.
* __Value:__ Represents literal values or constants in SQL queries, allowing users to include specific values directly in their queries.

### Q

The Q function empowers you to construct complex SQL expressions for filtering, querying, and creating conditions within database operations. Here's how users can utilize it:

__Basic Filtering with Single Condition__

```python
db.objects.filter('celebrities', Q(age__gt=20))
```

In this example, the Q function is used to create a filter condition where the 'age' column should be greater than 20 in the 'celebrities' table.

__Filtering with Multiple Conditions using Logical AND__

```python
db.objects.filter('celebrities', Q(age__gt=20) & Q(age__lt=30))
```

Here, the Q function constructs a filter condition with two criteria joined by a logical __AND__ operator. It filters rows from the 'celebrities' table where the 'age' column is greater than 20 and less than 30.

__Filtering with Multiple Conditions using Logical OR__

Similarly, the Q function is employed to create a filter condition with two criteria joined by a logical __OR__ operator. It retrieves rows from the 'celebrities' table where the 'age' column is either greater than 20 or less than 30.

__Complex Filtering with Multiple Conditions and Field Matching__

```python
db.objects.get('celebrities', Q(firstname__contains='Kendall', lastname__contains='J') & Q(country='USA'))
```

In this case, the Q function is used to create a complex filter condition combining multiple criteria with both logical AND and OR operators. It retrieves a single row from the 'celebrities' table where the 'firstname' contains 'Kendall', the 'lastname' contains 'J', and the 'country' is 'USA'.

__Inversion Operation__

```python
db.objects.filter('celebrities', ~Q(age__gt=20))
```

In this example, the tilde (~) operator is applied to the Q function, signifying an inversion operation. The inversion reverses the logical condition specified within the Q object. Here, the query retrieves rows from the 'celebrities' table where the 'age' column is not greater than 20.

[indexes](#indexes)

### F

You can utilize the `F` expression to reference database columns within SQL queries and perform arithmetic operations or other manipulations on their values.

__Single Column Reference and Arithmetic Operation__

```python
db.objects.annotate('celebrities', age_plus_one=F('age') + 1)
```

In this example, the `F` expression is used to reference the `age` column in the `celebrities` table. By adding 1 to the value of `age`, users can create a new alias column named `age_plus_one` in the query result, where each value is incremented by 1.

__Multiple Column Reference and Arithmetic Operation__

```python
db.objects.annotate('celebrities', age_plus_age=F('age') + F('age'))
```

Here, the F expression is used to reference the `age` column twice in the `celebrities` table. By adding the values of `age` together, you can create a new alias column named `age_plus_age` in the query result, where each value is the sum of the corresponding `age` values.

__Single Column Reference without Operation__

```python
db.objects.annotate('celebrities', age_alias=F('age'))
```

In this case, the F expression is used to reference the `age` column in the `celebrities` table without performing any arithmetic operation. This creates a new alias column named `age_plus_one` in the query result, where each value is the same as the original `age` column.

### Case

It corresponds to the SQL __CASE__ statement, allowing you to perform conditional evaluations and transformations on data. With the Case expression, you can define multiple conditions and their corresponding outcomes. This enables conditional logic within queries, such as determining different values based on specific conditions or criteria.

On the other hand, the `Where` expression represents the __WHEN ... THEN ...__ clauses within the Case expression. By combining `Where` clauses within a Case expression, you can create complex conditional logic and data transformations.

```python
from lorelie.expressions import When, Case
from lorelie.database.base import Database

# Define the condition using the When expression
logic = When(name='Kendall', then_case='Kandy')

# Create the Case expression with the defined condition
case = Case(logic)

# Annotate the 'celebrities' table with the Case expression
# The result will be stored in the 'other_name' alias column
db.objects.annotate('celebrities', other_name=case)

```

In this example, we first define the condition using the When expression, specifying that when the name is `Kendall`, the result should be `Kandy`. Then, we create the Case expression with the defined condition. Finally, we use the annotate method to apply the Case expression to the 'celebrities' table, assigning the result to the `other_name` alias column.

It's important to note that the Case expression should only be used in conjunction with the `annotate` method, as it generates an alias column in the database with the result of the condition.

## Lookup Filters
