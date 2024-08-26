<template>
  <div class="row">
    <div class="col-12">
      <base-card class="mb-2" card-id="database" title="The database">
        <template #default>
          <p>
            The Database class in Lorelie is the main wrapper responsible for creating and 
            organizing tables, indexes, constraints, and other database structures. It serves as 
            the central point for managing database schema and executing migrations, ensuring that 
            your database stays in sync with your application models. By using the Database class, you 
            can easily define and manipulate the core components of your database, facilitating efficient 
            and structured data management.
          </p>

          <h5 id="#setting-database">Setting up your database</h5>
          
          <code>
            <pre class="bg-light p-4 rounded-1">
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
            </pre>
          </code>

          <p>
            By default, when creating a Table instance, a connection to the SQLite database is 
            not automatically established. The connection and table creation are handled by the 
            migrate method of the Database class.

            The Database class is the main wrapper for creating and organizing tables, indexes, constraints, 
            and other database objects. It manages multiple tables in a database and provides additional 
            functionalities such as migrations, which ensure that your database schema is synchronized with 
            your application models.

            The migrate method establishes the connection to the database, creates the tables, indexes, and 
            applies any defined constraints.
          </p>
        </template>
      </base-card>

      <base-card class="mb-2" card-id="database-manager" title="The database manager">
        <template #default>
          The Database provides a DatabaseManager class which serves as an endpoint for 
          interacting with the database. You can perform various operations such as creating, 
          fetching, updating, and deleting data within the database tables using db.objects. Here's a 
          breakdown of the functionalities provided by the DatabaseManager:

          <p class="fw-bold">CRUD Operations</p>
          <ul>
            <li>Create: Adds new rows to the specified table</li>
            <li>Filter: Filters rows based on specified conditions</li>
            <li>Get: Retrieves a single row based on specified conditions</li>
            <li>Annotate: Adds annotations or computed fields to the queryset</li>
            <li>Values: Returns data from the database in the form of a list of dictionaries</li>
            <li>DataFrame: Converts queryset data into a pandas DataFrame</li>
            <li>Bulk Create: Inserts multiple rows into the database at once</li>
          </ul>

          <p class="fw-bold">Queryset Operations</p>
          <ul>
            <li>All: Retrieves all rows from the specified table</li>
            <li>Order By: Orders the queryset based on specified fields</li>
            <li>Count: Counts the total number of rows in the specified table</li>
          </ul>

          <p class="fw-bold">Data Retrieval</p>
          <ul>
            <li>First: Retrieves the first row from the specified table</li>
            <li>Last: Retrieves the last row from the specified table</li>
            <li>Earliest: Retrieves the row with the earliest date or datetime</li>
            <li>Latest: Retrieves the row with the latest date or datetime</li>
            <li>Additional Operations</li>
            <li>Dates: Retrieves unique dates from a datetime field</li>
            <li>Difference: Finds the difference between two querysets</li>
            <li>Distinct: Removes duplicate rows from the queryset</li>
            <li>Only: Retrieves queryset with only specified fields</li>
            <li>Exclude: Excludes rows based on specified conditions</li>
            <li>Extra: Performs additional operations on the queryset</li>
          </ul>
        </template>
      </base-card>
    </div>
  </div>
</template>

<script>
import _ from 'lodash'
import { ref } from 'vue'

import BaseCard from 'src/components/BaseCard.vue'

export default {
  name: 'HomePage',
  setup () {
    const titles = ref([])
    return {
      titles
    }
  },
  components: {
    BaseCard
  },
  beforeMount () {
    const title = document.querySelectorAll('h3')
    console.log(title)
    // _.forEach(title, (item) => {
    //   this.titles.push(item)
    // })

    // const script1 = document.createElement('script')
    // const script2 = document.createElement('script')

    // const srcNode1 = document.createAttribute('src')
    // srcNode1.value = "https://cdn.jsdelivr.net/npm/codemirror@5.61.0/lib/codemirror.min.js"
    // script1.setAttributeNode(srcNode1)
    
    // const srcNode2 = document.createAttribute('src')
    // srcNode2.value = "https://cdn.jsdelivr.net/npm/codemirror@5.61.0/mode/python/python.min.js"
    // script2.setAttributeNode(srcNode2)

    // document.head.appendChild(script1)
    // document.head.appendChild(script2)
  }
}
</script>

<style scoped>
</style>
