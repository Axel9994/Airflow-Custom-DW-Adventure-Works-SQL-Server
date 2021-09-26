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

