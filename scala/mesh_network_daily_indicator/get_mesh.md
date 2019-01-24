// GET TRANSFORMER 

var df_transformer =  get_transformer_bank(spark)

<!-- scala> df_transformer.printSchema
root
 |-- transformer_bank_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 //GET MESH NETWORK 
 var df_mesh = get_mesh_grid(spark)

 <!-- scala> df_mesh.printSchema
root
 |-- mesh_grid_id: string (nullable = true)
 |-- state: string (nullable = true)
 |-- snapshot_year: string (nullable = true)
 |-- snapshot_month: string (nullable = true)
 |-- snapshot_day: string (nullable = true) -->

 // JOIN TRANSFORMER AND MESH 
