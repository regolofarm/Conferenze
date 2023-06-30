  DELETE FROM [Contoso].[sqlsat921].[Sales]
  Where YEAR(Sales.[Datekey]) = 2007 

---Ricarico le righe
SELECT [DateKey]
      ,[ProductKey]
      ,[CustomerKey]
      ,[Quantity]
      ,[Amount]
      ,[Cost]
  INTO [sqlsat921].[Sales]
  FROM [Contoso].[sqlsat921].[Sales_2007]
  Where YEAR(Sales.[Datekey]) = 2007 