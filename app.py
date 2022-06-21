from pyspark.sql import SparkSession
from pyspark.sql.functions import count, concat_ws, sum, rank, desc
from pyspark.sql.window import Window

# create instance of SPARK
spark = (
    SparkSession.builder.appName("Task_3")
    .config("spark.jars", "postgresql-42.3.6.jar")
    .getOrCreate()
)

# create reader to read data from DB
reader = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
)

# reading data from DB
film_df = reader.option("dbtable", "film").load()
film_category_df = reader.option("dbtable", "film_category").load()
category_df = reader.option("dbtable", "category").load()
actor_df = reader.option("dbtable", "actor").load()
film_actor_df = reader.option("dbtable", "film_actor").load()
inventory_df = reader.option("dbtable", "inventory").load()
rental_df = reader.option("dbtable", "rental").load()
payment_df = reader.option("dbtable", "payment").load()
customer_df = reader.option("dbtable", "customer").load()
address_df = reader.option("dbtable", "address").load()
city_df = reader.option("dbtable", "city").load()
film_list_df = reader.option("dbtable", "film_list").load()
customer_list_df = reader.option("dbtable", "customer_list").load()


# 	1. Number of films in each category in descending order.

print("Number of films in each category in descending order.")
film_df.\
    join(film_category_df, ["film_id"]).\
    join(category_df, ["category_id"]).\
    select(category_df.name.alias("category")).\
    groupBy("category").\
    agg(count("category").alias("amount")).\
    orderBy("amount", ascending=False).\
    show()

# 	2. Top 10 actors whose films are most rented in descending order.

print("Top 10 actors whose films are most rented in descending order.")
actor_df.\
    join(film_actor_df, ["actor_id"]).\
    join(inventory_df, ["film_id"]).\
    join(rental_df, ["inventory_id"]).\
    select(concat_ws(" ", actor_df.first_name, actor_df.last_name).alias("actor name")).\
    groupBy("actor name").\
    agg(count("actor name").alias("amount")).\
    orderBy("amount", ascending=False).\
    show(10)

# 	3. Film category with most money spent.

print("Film category with most money spent.")
category_df.\
    join(film_category_df, ["category_id"]).\
    join(inventory_df, ["film_id"]).\
    join(rental_df, ["inventory_id"]).\
    join(payment_df, ["rental_id"]).\
    select(category_df.name.alias("category"), payment_df.amount).\
    groupBy("category").\
    agg(sum(payment_df.amount).alias("amount")).\
    orderBy("amount", ascending=False).\
    show(1)

# 	4. Films not in inventory, without using IN.

print("Films not in inventory, without using IN.")
film_df.\
    join(inventory_df, ["film_id"], "left").\
    select(film_df.title).\
    filter(inventory_df.inventory_id.isNull()).\
    show()

#    5. Top 3 actors in category "children" with ties.

# To get data with ties all aggregated data is ranked.

print("Top 3 actors in category 'children' with ties.")
actor_df.\
    join(film_actor_df, ["actor_id"]).\
    join(film_category_df, ["film_id"]).\
    join(category_df, ["category_id"]).\
    select(concat_ws(" ", actor_df.first_name, actor_df.last_name).alias("actor name")).\
    filter(category_df.name == "Children").\
    groupBy("actor name").\
    agg(count("actor name").alias("amount")).\
    withColumn("rank", rank().over(Window.partitionBy().orderBy(desc("amount")))).\
    filter("rank< 4").\
    show()

# 6. Cities with active and inactive clients. Sort by inactive clients in descending order.

print("Cities with active and inactive clients. Sort by inactive clients in descending order.")
customer_df.\
    join(address_df, ['address_id']).join(city_df, ['city_id']).\
    select(city_df.city,customer_df.active.alias('active')).\
    groupBy(city_df.city).\
    agg(sum('active').alias('active'),(count('active')-sum('active')).alias('inactive')).\
    orderBy('inactive', ascending=False).\
    show()


# 	7. Category with most hours rent in cities begin with 'a'. the same for cities which have '-' in their name. 

# Here kank is used to get the top category for cities with certain condition.

print("Category with most hours rent in cities begin with 'a'. the same for cities which have '-' in their name.")
customer_list_df.\
    join(rental_df, rental_df.customer_id == customer_list_df.id).\
    join(inventory_df, ['inventory_id']).\
    join(film_list_df, film_list_df.fid == inventory_df.film_id).\
    filter((rental_df.return_date-rental_df.rental_date).isNotNull() & (customer_list_df.city.rlike("^A") | customer_list_df.city.rlike("-"))).\
    groupBy(customer_list_df.city, film_list_df.category).\
    agg(sum(rental_df.return_date-rental_df.rental_date).alias('rent_time')).\
    withColumn('rank', rank().over(Window.partitionBy(customer_list_df.city).orderBy(desc('rent_time')))).\
    select(customer_list_df.city, film_list_df.category,('rent_time')).\
    filter('rank==1').\
    show()
