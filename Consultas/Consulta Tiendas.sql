create view [dbo].[vTiendas] as
SELECT distinct c.StoreID,
	   st.Name
  FROM [AdventureWorks2019].[Sales].[Customer] c
  left join Sales.Store st
  on c.StoreID = st.BusinessEntityID
  where StoreID is not null

