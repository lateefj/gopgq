===
why
===

.. image:: https://i2.imgflip.com/1srxly.jpg

Why benchmark message queues using SQL databases isn't that a big no no? In the past I myself have probably given this advice not to use a relational database as a message queue. Lately I have been thinking a lot about storing state. I work a lot with high throughput message systems (specifically using Kafka_). As I get closer to my second decade in developing software for the web I get a fair amount of questions about architectural choices. Running something like Kafka_ is PITA even if it is a managed service. Just to list a couple of this reason to use Postgresql:

- Postgresql is a great RDBMS 
- Operating RDBMS is well known and understood
- Monitoring Postgresql is fantastic it has built in SQL queries in addition lots of third party tools
- It is trivial to debug using SQL

In general I have found most web software has a database some place to store state. Postgresql is usually what I for production systems (Prototyping I usually recommend a document store like Mongo or redis). Anyway since Postgresql is a well understand easy to run service I started to wonder


.. _Kafka: https://kafka.apache.org/



