from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

default_args = {
    'owner': 'Axel Aleman',
    'start_date': days_ago(7)
}

dag_args = {
    'dag_id': 'CustomETL_DAG',
    'schedule_interval': '@once',
    'catchup': False,
    'default_args': default_args
}

with DAG(**dag_args) as dag:
    territory_query = '''
    create view vTerritorios as
select ROW_NUMBER() over(ORDER BY datos.City, datos.StateProvinceID, datos.State_Name, datos.Territory_Name) as CityID,
       datos.City,
	   datos.StateProvinceID,
	   datos.State_Name,
	   datos.Territory_Name
from (SELECT distinct 
                ad.[City],
                ad.[StateProvinceID],
				stp.Name as State_Name,
				slt.Name as Territory_Name
  FROM [AdventureWorks2019].[Person].[Address] ad
  left join Person.StateProvince stp
  on ad.StateProvinceID = stp.StateProvinceID
  left join Sales.SalesTerritory slt
  on stp.TerritoryID = slt.TerritoryID) datos
    '''

    tienda_query = '''
    create view [dbo].[vTiendas] as
SELECT distinct c.StoreID,
	   st.Name
  FROM [AdventureWorks2019].[Sales].[Customer] c
  left join Sales.Store st
  on c.StoreID = st.BusinessEntityID
  where StoreID is not null
    '''

    vendedor_query = '''
    create view vVendedores as
SELECT sp.[BusinessEntityID] as VendedorID,
       emp.JobTitle,
	   p.FirstName,
	   p.LastName
  FROM [AdventureWorks2019].[Sales].[SalesPerson] sp
  left join HumanResources.Employee emp
  on sp.BusinessEntityID = emp.BusinessEntityID
  left join Person.Person p
  on sp.BusinessEntityID = p.BusinessEntityID
    '''

    ingreso_query = '''
    create view [dbo].[vIngreso] as
select detalle_costo.SalesOrderID,
	   cabecera.DueDate as Fecha_Entrega,
	   consu.StoreID,
	   cabecera.SalesPersonID,
	   te.CityID,
       detalle_costo.total_costo,
	   cabecera.TotalDue as Total_venta_um,
	   COALESCE(tasa_cambio.AverageRate, 1) as tasa_promedio
from (select 
			detalle.SalesOrderID, 
			sum(detalle.OrderQty * producto.StandardCost) as total_costo
from Sales.SalesOrderDetail detalle
join Production.Product producto
on detalle.ProductID = producto.ProductID
group by detalle.SalesOrderID) detalle_costo
join Sales.SalesOrderHeader cabecera
on detalle_costo.SalesOrderID = cabecera.SalesOrderID
left join Sales.CurrencyRate tasa_cambio
on cabecera.CurrencyRateID = tasa_cambio.CurrencyRateID
left join Sales.Customer consu
on cabecera.CustomerID = consu.CustomerID
left join Person.BusinessEntityAddress bea
on bea.BusinessEntityID = consu.StoreID
left join Person.Address ad
on bea.AddressID = ad.AddressID
left join dbo.vTerritorios te
on ad.City = te.City and ad.StateProvinceID = te.StateProvinceID
where cabecera.CustomerID in (select CustomerID from Sales.Customer where StoreID is not null)
    '''

    crear_dim_territorio = MsSqlOperator(sql = territory_query, mssql_conn_id = 'mssql_default')

    crear_dim_tienda = MsSqlOperator(sql = tienda_query, mssql_conn_id = 'mssql_default')

    crear_dim_vendedor = MsSqlOperator(sql = vendedor_query, mssql_conn_id = 'mssql_default')

    crear_fact_ingreso = MsSqlOperator(sql = ingreso_query, mssql_conn_id = 'mssql_default')

    [crear_dim_territorio, crear_dim_tienda, crear_dim_vendedor] >> crear_fact_ingreso

    