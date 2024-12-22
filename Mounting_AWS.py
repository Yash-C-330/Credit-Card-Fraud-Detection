# Databricks notebook source

access_key = ""
secret_key = ""
spark.conf.set("fs.s3a.access.key",access_key)
spark.conf.set("fs.s3a.secret.key",secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
