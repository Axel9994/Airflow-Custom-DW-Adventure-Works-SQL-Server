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