const oracledb = require('oracledb');
const axios = require('axios');
const logger = require('./logger');
const { response, json } = require('express');
var cron = require('node-cron');

let globalAccessToken = null; // Global variable to store the access token
const now = new Date(); 
// Function to establish a database connection
async function getDbConnection() {
    try {
        const connection = await oracledb.getConnection({
            user: "oracleUserHere",
            password: "passwordHere",
            connectString: "Paste Connection String Here",
        });
        sendLogUpdates("Date : " + now.toString() + "Database Success Log :: Connection successful!", true);
        console.log('Connection successful!');
        return connection;
    } catch (err) {
        console.error('Error connecting to the database:', err);
        sendLogUpdates("Date : " + now.toString() + "Database Error Log :: Connection Failed!", true);
        throw err; // Rethrow the error to handle it in the calling function
    }
}

// Function to check if a connection is active
function isConnectionActive(connection) {
    return connection && connection.isHealthy && connection.isHealthy();
}

// Function to execute Product Master query and return the result as JSON
async function executeProductMaster(connection) {
    try {
        if (!isConnectionActive(connection)) {
            console.log('Database connection is not active.');
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Database connection is not active", false);
            //throw new Error('Database connection is not active.');
        }
        const query = `
        select a.it_desc as "Product Name"
           ,a.it_code as "Product Code",a.it_spec as "Item Specification"
           , 'true' as Active
           ,decode(a.it_type,'1','1-Raw Material','2','2-Bought Out Material','3','3-Single made in components') as "Item Type"
           ,(select aa.ig_code || '-' || aa.ig_name from igmast aa where aa.comp_code = 'SH' and aa.ig_code = a.it_class) as "Item Classification"      
           ,(select bb.ig_code || '-' || bb.ig_name from igmast bb where bb.comp_code = 'SH' and bb.ig_code = a.it_group) as "Item Group"
           ,(select cc.catg_code ||'-' || cc.CATG_NAME from catgmast cc where cc.catg_code = 'FG' and cc.comp_code ='SH' and cc.catg_code = a.it_cat)  as "Item Category"
           ,a.it_unit as "UOM",b.IT_DRNO as "Drawing No"
           ,b.IT_RVNO as "Rev No",
           a.CREATED_DT as created_at
        from itmast a, itmastqc b
        where a.it_code = b.it_code
        and a.comp_code = b.comp_code
        and a.comp_code = 'SH'
        and a.it_cat = 'FG'
        and a.IT_OPRT = 'Y'
        `;//Need to add date condition here

        const result = await connection.execute(query);
        const jsonResult = result.rows.map(row => {
            return {
                Name: row[0],
                ProductCode: row[1],
                IsActive: row[3],
                AL_UOM_c: row[8],
                AL_Item_Type_c: row[4],
                AL_Item_Classification_c: row[5],
                AL_Item_Group_c: row[6],
                AL_Item_Category_c: row[7],
                AL_Drawing_No_c: row[9],
                AL_Rev_No_c: row[10],
                Description: row[2],
                CurrencyIsoCode: "INR"
            };
        });

        const productMaster = {
            "ProductDetails": jsonResult
        };
        let masterJson = JSON.stringify(productMaster)
        await setproductMasterData(masterJson); // Store the product master data
    } catch (err) {
        console.error('Error executing the query:', err);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Error executing the product master query, "+err, false);
        throw err; // Rethrow the error to handle it in the calling function
    }
}

// Function to excute Customer Master Query and return result as a JSON

async function executeCustomerMaster(connection) {
    try {
        if (!isConnectionActive(connection)) {
            console.log('Database connection is not active.');
            //throw new Error('Database connection is not active.');
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Database connection is not active", false); 
        }
        const customerQuery = `select  aa.prt_name as "Account Name"
        ,aa.prt_code as "Cust Code",aa.prt_add as "Billing Street"
        ,F_GET_CITY_STATE_CNT('CN',aa.prt_city,'SH') AS "Billing City"
        ,F_GET_CITY_STATE_CNT('SN',aa.prt_city,'SH') AS "Billing State"
        ,aa.PRT_PIN as "Billing Zip"
        ,F_GET_CITY_STATE_CNT('CNTN',aa.prt_city,'SH') AS "Billing Country"
        ,aa.prt_tel as "Account Phone"
        ,aa.PRT_URL as "Website"
        ,aa.PRT_CUR as "Account Currency"
        ,aa.PRT_PAN as "PAN Number"
        ,decode(aa.prt_cat,'A01','CUSTOMER','OTHER') as "Category"
        ,'true' as "From ERP"
        ,aa.PRT_GSTNO as "GST No"
        ,decode(aa.prt_group,'06','Export Client','01','Local Client') as "Party Group"
        from prtmast aa
        where aa.prt_type = 'C'
        and aa.prt_group in ('01','06')
        and aa.comp_code = 'SH'
        and ((to_char(aa.created_dt,'YYYYMMDD') >= '20140401')or to_char(aa.modified_dt,'YYYYMMDD') >= '20140401')
        `;

        const CustomerResult = await connection.execute(customerQuery);
        
        const CustomerJsonResult = CustomerResult.rows.map(custRow => {
            return {
                Name : custRow[0],
                AL_Cust_Code_c : custRow[1],
                BillingStreet : custRow[2],
                BillingCity : custRow[3],
                BillingState : custRow[4],
                BillingPostalCode : custRow[5],
                BillingCountry : custRow[6],
                Phone : custRow[7],
                Website : custRow[8],
                CurrencyIsoCode : custRow[9],
                AL_Pan_Number_c : custRow[10],
                AL_Category_c : custRow[11],
                From_ERP_c : custRow[12],
                AL_GST_No_c : custRow[13],
                AL_Party_Group_c : custRow[14],
            };
        });
        const customerMaster = {
            "AccountDetails": CustomerJsonResult
        };
        let customerMasterJson = JSON.stringify(customerMaster)
        await setCustomerMasterData(customerMasterJson); // Store the product master data
    } catch (err) {
        console.error('Error executing the customer master query:', err);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Error executing the customer master query, "+err, false);
        throw err; // Rethrow the error to handle it in the calling function
    }
}

// Function to excute Sales Order Query and return result as a JSON

async function executeSalesOrder(connection) {
    try {
        if (!isConnectionActive(connection)) {
            console.log('Database connection is not active.');
            //throw new Error('Database connection is not active.');
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Database connection is not active", false); 
        }
        const salesOrderQuery = `SELECT  distinct
                    --------------- SALES ORDER HEADER DATA -----------
                    PLOC_CODE AS "LOCATION CODE",
                    TXN_SRNO  AS "SO NO.", 
                    TXN_DATE  AS "SO DATE",
                    TXN_AMND AS "AMENDMEND NO",
                    TXN_AMDT AS "AMD DATE",
                    PRT_NAME AS "AL ACCOUNT NAME",
                    PRT_CODE AS "CUST CODE",
                    TXN_REF2 AS "QUOTE NUMBER",
                    TXN_RFDT2 AS "QUOTE DATE",
                    TXN_REF1 AS "CUSTOMER ORDER NO",
                    TXN_RFDT1 AS "CUSTOMER ORDER DATE",
                    --"TRUE" AS "ORDER RECEIVED FROM ERP",
                    ORDER_TYPE AS "ORDER TYPE",
                    TXN_NETT AS "PO NET AMOUNT",
                    TXN_AMT AS "PO GROSS AMOUNT",
                    --------------- SALES ORDER LINE DATA -----------
                    TXD_RUNO AS "LINE SR.NO.",
                    TXD_PORUNO AS "POSITION NO",
                    IT_CODE AS "PRODUCT CODE",
                    IT_DESC AS "PRODUCT NAME",
                    TXD_TLCCD AS "ITEM SPECIFICATION",
                    decode(it_type,'1','1-Raw Material','2','2-Bought Out Material','3','3-Single made in components') as "Item Type"
            ,(select aa.ig_code || '-' || aa.ig_name from igmast aa where aa.comp_code = 'SH' and aa.ig_code = q.it_class) as "Item Classification"
            ,(select bb.ig_code || '-' || bb.ig_name from igmast bb where bb.comp_code = 'SH' and bb.ig_code = q.it_group) as "Item Group"
            ,(select cc.catg_code ||'-' || cc.CATG_NAME from catgmast cc where cc.catg_code = 'FG' and cc.comp_code ='SH' and cc.catg_code = q.it_cat)  as "Item Category"
                    ,TXN_CURR ,
                    IT_DRNO AS "DRAWING NO",
                    IT_RVNO AS "DRAWING REV. NO", 
                    ORDER_QTY AS "ORDER QTY",
                    TXD_UNIT AS "UOM",
                    TXD_RATE AS "RATE",
                    TXD_AMT AS "AMOUNT"	
            FROM
            (
            (SELECT A.PLOC_CODE,
                    TXN_REF2, 
                    TXN_RFDT2,
                DECODE(A.SSEG_CODE,'SO01','LOCAL_ORDER','SO02','EXPORT_ORDER','SO05','LABOUR-ORDER','SO08','FORECAST-ORDER','SO12','BRANCH - ORDER',A.SSEG_CODE) AS ORDER_TYPE,
                    A.TXN_SRNO,
                    A.TXN_DATE,
                    A.TXN_REF1,
                    A.TXN_RFDT1,
                    A.TXN_AMND
                    ,A.TXN_AMDT,
                    B.TXD_RUNO,
                    B.TXD_PORUNO,
                    E.PRT_CODE,
                    E.PRT_NAME,
                    D.IT_CODE,
                    D.IT_DESC,
                    B.TXD_TLCCD,
                    G.IT_DRNO,
                    G.IT_RVNO,
                    C.TXD_QTY1 * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) ORDER_QTY,
                    B.TXD_UNIT,
                --	C.TXD_DUDT,
                --	C.TXD_QTY3 * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) DESP_QTY,
                --	(NVL(C.TXD_QTY8,0) + NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) SHORTCLOSE_QTY,
                --	(C.TXD_QTY1 - NVL(C.TXD_QTY3,0)- NVL(C.TXD_QTY8,0)- NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) PENDING_QTY,
                    TXD_RATE  * TXN_EXCHRT TXD_RATE,
                    ((C.TXD_QTY1 - NVL(C.TXD_QTY3,0)- NVL(C.TXD_QTY8,0)- NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) ) * B.TXD_RATE *  TXN_EXCHRT / DECODE(NVL(B.TXD_QTPRT,0),0,1,B.TXD_QTPRT) TXD_AMT,
                --	NIS.F_B2B_FINDNAME('U',A.CREATED_BY,A.COMP_CODE) USR
                    D.IT_TYPE,
                    d.it_group
                    ,D.IT_CLASS
                    ,A.TXN_CURR
                    ,D.IT_CAT
                    ,a.txn_amt
                    ,a.TXN_NETT
            FROM NIS.MMMMAST A, NIS.MMDMAST B, NIS.ITMAST D, NIS.PRTMAST E, NIS.SSEGMAST F, NIS.MMDDUDT C, NIS.ITMASTQC G
            WHERE A.COMP_CODE = 'SH' --:GLOBAL.COMP
            AND A.TXN_DOC='SO'
            AND NVL(B.TXD_STAT,'O') = 'O'
            AND C.TXD_SEQ = B.TXD_SEQ
            AND C.TXD_RUNO = B.TXD_RUNO
            AND C.TXD_RDNO = B.TXD_RDNO
            AND B.TXD_SEQ=A.TXN_SEQ
            AND B.TXD_RDNO=0
            AND D.COMP_CODE=B.COMP_CODE
            AND D.IT_CODE=B.TXD_ITEM
            AND E.COMP_CODE=A.COMP_CODE
            AND E.PRT_CODE=A.TXN_ACCD
            AND E.PRT_CAT = 'A01'
            AND F.COMP_CODE=A.COMP_CODE
            AND F.SSEG_CODE=A.SSEG_CODE
            AND G.COMP_CODE (+) =D.COMP_CODE
            AND G.IT_CODE (+) = D.IT_CODE
            AND INSTR(A.POSTED_BY , '---') = 0
            AND INSTR(SSEG_DTYPE , 'SOOO') = 0
            AND to_char(A.POSTED_DT,'YYYY-MM-DD') >='2024-08-01'
            AND ((C.TXD_QTY1-C.TXD_QTY3-NVL(C.TXD_QTY8,0)-NVL(C.TXD_QTY9,0) > 0 AND NVL(B.TXD_STAT,'O') = 'O' AND NVL(A.TXN_STAT,'O') = 'O' ))
            AND F.SSEG_DTYPE <> 'SOST'
            --AND A.PLOC_CODE = '#QP01'
            --AND TO_CHAR(C.TXD_DUDT,'YYYYMMDD') <= '#QP02'
            --AND ('#QP03' IS NULL OR E.PRT_CODE = '#QP03')
            AND A.TXN_DIVN <> 'SHU-MEZ'
            AND B.TXD_TLCCD IS NULL )
            UNION ALL
            (SELECT A.PLOC_CODE,
                    TXN_REF2, 
                    TXN_RFDT2,
                DECODE(A.SSEG_CODE,'SO01','LOCAL_ORDER','SO02','EXPORT_ORDER','SO05','LABOUR-ORDER','SO08','FORECAST-ORDER','SO12','BRANCH - ORDER',A.SSEG_CODE) AS ORDER_TYPE,
                    A.TXN_SRNO,
                    A.TXN_DATE,
                    A.TXN_REF1,
                    A.TXN_RFDT1,
                    A.TXN_AMND
                    ,A.TXN_AMDT,
                    B.TXD_RUNO,
                    B.TXD_PORUNO,
                    E.PRT_CODE,
                    E.PRT_NAME,
                    D.IT_CODE,
                    D.IT_DESC,
                    B.TXD_TLCCD,
                    G.IT_DRNO,
                    G.IT_RVNO,
                    C.TXD_QTY1 * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) ORDER_QTY,
                    B.TXD_UNIT,
                --	C.TXD_DUDT,
                --	C.TXD_QTY3 * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) DESP_QTY,
                --	(NVL(C.TXD_QTY8,0) + NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) SHORTCLOSE_QTY,
                --	(C.TXD_QTY1 - NVL(C.TXD_QTY3,0)- NVL(C.TXD_QTY8,0)- NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) PENDING_QTY,
                    TXD_RATE  * TXN_EXCHRT TXD_RATE,
                    ((C.TXD_QTY1 - NVL(C.TXD_QTY3,0)- NVL(C.TXD_QTY8,0)- NVL(C.TXD_QTY9,0)) * B.TXD_QTY5/DECODE(B.TXD_QTY1,0,1,B.TXD_QTY1) ) * B.TXD_RATE *  TXN_EXCHRT / DECODE(NVL(B.TXD_QTPRT,0),0,1,B.TXD_QTPRT) TXD_AMT,
                --	NIS.F_B2B_FINDNAME('U',A.CREATED_BY,A.COMP_CODE) USR
                    D.IT_TYPE
                    ,d.it_group
                    ,D.IT_CLASS 
            ,A.TXN_CURR
            ,D.IT_CAT
            ,a.txn_amt
            ,a.TXN_NETT
            FROM NIS.MMMMAST A, NIS.MMDMAST B, NIS.ITMAST D, NIS.PRTMAST E, NIS.SSEGMAST F, NIS.MMDDUDT C, NIS.ITMASTQC G
            WHERE A.COMP_CODE = 'SH' 
            AND A.TXN_DOC='SO'
            AND NVL(B.TXD_STAT,'O') = 'O'
            AND C.TXD_SEQ = B.TXD_SEQ
            AND C.TXD_RUNO = B.TXD_RUNO
            AND C.TXD_RDNO = B.TXD_RDNO
            AND B.TXD_SEQ=A.TXN_SEQ
            AND B.TXD_RDNO=0
            AND D.COMP_CODE=B.COMP_CODE
            AND D.IT_CODE=B.TXD_ITEM
            AND E.COMP_CODE=A.COMP_CODE
            AND E.PRT_CODE=A.TXN_ACCD
            AND F.COMP_CODE=A.COMP_CODE
            AND E.PRT_CAT = 'A01'
            AND F.SSEG_CODE=A.SSEG_CODE
            AND G.COMP_CODE (+) =D.COMP_CODE
            AND G.IT_CODE (+) = D.IT_CODE
            AND INSTR(A.POSTED_BY , '---') = 0
            AND INSTR(SSEG_DTYPE , 'SOOO') = 0
            --AND to_char(A.POSTED_DT,'YYYY-MM-DD') >='2024-08-01'
            AND ((C.TXD_QTY1-C.TXD_QTY3-NVL(C.TXD_QTY8,0)-NVL(C.TXD_QTY9,0) > 0 AND NVL(B.TXD_STAT,'O') = 'O' AND NVL(A.TXN_STAT,'O') = 'O' ))
            AND F.SSEG_DTYPE <> 'SOST'
            --AND A.SSEG_CODE ='SO02'
            --AND B.TXD_TLCCD = '#QP01'
            --AND TO_CHAR(C.TXD_DUDT,'YYYYMMDD') <= '#QP02'
            --AND ('#QP03' IS NULL OR E.PRT_CODE = '#QP03')
            AND A.TXN_DIVN <> 'SHU-MEZ'
            )
            ) q WHERE TXN_SRNO = 'SHN25X000014'`;

          

        const salesOrderResult = await connection.execute(salesOrderQuery);
        
        if(salesOrderResult.rows.length > 0){
            const groupedData = salesOrderResult.rows.reduce((acc, curr) => {
                const key = curr[1];
    
                // Initialize if the group doesn't exist yet
                if (!acc[key]) {
                    // First part (index 0 to 13), and an array to store the differing values (index 14 to 29)
                    acc[key] = {
                        common: curr.slice(0, 14),
                        differing: []
                    };
                }
                
                // Push the differing part (index 14 to 29) into the differing array
                acc[key].differing.push(curr.slice(14));
    
                return acc;
            }, {});
            
            const orderResults = Object.values(groupedData).map(group => {
                return {
                    common: group.common,        // Common part (indices 0 to 13)
                    differing: group.differing   // All differing parts (indices 14 to 29)
                };
            });
    
            let OrderJsonResult = [];
    
            for (let i = 0; i < orderResults.length; i++) {
                const orderRow = orderResults[i];
              
                let productData = orderRow.differing.map(productRow => {
                    return {
                        "Txd_Dudt_c": null,
                        "Txd_Tlccd_c": productRow[4],
                        "Usr_c": null,
                        "Item_Classification_c": productRow[6],
                        "ProductCode": productRow[2],
                        "It_Specification_c": productRow[3],
                        "CurrencyIsoCode": productRow[9],
                        "Txd_Runo_c": productRow[0],
                        "Txd_Poruno_c": productRow[1],
    //                     "It_Drno_c": productRow[10],
    //                     "It_Rvno_c": productRow[11],
                        "Shortclose_Qty_c": null,
                        "Desp_Qty_c": null,
                        "Pending_Qty_c": null,
                        "Order_Qty_c": productRow[12],
                        "Txd_Rate_c": productRow[14],
                        "UOM": productRow[13]
                    };
                });
    
                OrderJsonResult.push({
                        So_No: orderRow.common[1],
                        CustCode: orderRow.common[6],
                        quoteSrNo: "SHN25Q00000022",
                        CurrencyIsoCode: orderRow[23],
                        Status: null,
                        AL_Ploc_Code_c: orderRow.common[0],
                        AL_Order_Type_c: orderRow.common[11],
                        AL_Txn_Date_c: orderRow.common[2] ? new Date(orderRow.common[2]).toISOString().split('T')[0] : null,
                        AL_Amd_Date_c: orderRow.common[4] ? new Date(orderRow.common[4]).toISOString().split('T')[0] : null,
                        AL_Txn_Ref1_c: orderRow.common[9],
                        AL_Amd_No_c: orderRow.common[3],
                        Description: null,
                        ordertype: orderRow.common[11],
                        TXN_RFDT1_c: orderRow.common[10] ? new Date(orderRow.common[10]).toISOString().split('T')[0] : null,
                        OrderAmount: orderRow.common[12],
                        QuoteNumber: orderRow.common[7],
                        PODate:orderRow.common[10] ? new Date(orderRow.common[10]).toISOString().split('T')[0] : null,
                        PONumber:"11111",
                        POValue:orderRow.common[13],
                        OrderProducts:productData
                })
            }
    
            const orderMaster = {
                "OrderDetails": OrderJsonResult
            };
            let orderJson = JSON.stringify(orderMaster);
            console.log("JSON Data: "+orderJson)
            await setSalesOrderData(orderJson); // Store the product master data
        }
        else{
            sendLogUpdates("Date : " + now.toString() + " Success Log :: No data found", true);
        }
        
        
    } catch (err) {
        console.error('Error executing the sales order query:', err);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Error executing the sales order query, "+err, false);
        throw err; // Rethrow the error to handle it in the calling function
    }
}

// Function to execute invoice data query and return result as a JSON

async function executeInvoiceMasterData(connection){
    try {
        if (!isConnectionActive(connection)) {
            console.log('Database connection is not active.');
            //throw new Error('Database connection is not active.');
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Database connection is not active", false); 
        }
        const invoiceQuery = `SELECT DISTINCT A.TXN_ACCD AS "CUST CODE"
                    ,(SELECT DISTINCT P.PRT_NAME FROM PRTMAST P WHERE P.PRT_CODE = A.TXN_ACCD AND P.COMP_CODE = A.COMP_CODE ) AS "NAME"
                    ,A.TXN_SRNO AS "INVOICE NO",A.TXN_DATE AS "INV DATE",A.TXN_CURR AS "CURR"
                    ,C.TXD_EXCHRT AS "EXCHANGE RATE"
                    ,C.TXD_AMTFC AS "INVOICE AMT (FC)"
                    ,C.TXD_AMT AS "INVOICE AMT (INR)"
                    ,C.TXD_CRDR AS "TXD_CRDR"
                    ,C.TXD_OUTS AS "OUTSTANDING(INR)"
                    ,C.TXD_OUTSFC AS "OUTSTANDING(FC)"
                    FROM MMMMAST A,FAMAST B,FADADJ C
                    WHERE A.TXN_SEQ = B.TXN_SEQ
                    AND B.TXN_SEQ = C.TXD_SEQ
                    AND C.COMP_CODE = B.COMP_CODE
                    AND A.COMP_CODE = 'SH'
                    AND A.TXN_DOC = 'IV'
                    AND A.SSEG_CODE IN ('IV01','IV03','IV04')
                    AND A.TXN_DIVN <> 'SHU-MEZ'
                    AND TO_CHAR(A.TXN_DATE,'YYYYMMDD') >= '20240401'
                    --AND A.TXN_SRNO ='SHS25E000372'
                    AND TXD_RUNO = 0
                    ORDER BY 1 DESC`;

        const invoiceResult = await connection.execute(invoiceQuery);
        const invoiceJsonResult = invoiceResult.rows.map(invoiceRow => {
            return {
                INVOICE_NO: invoiceRow[2] ? invoiceRow[2] : "",
                INVOICE_DATE: invoiceRow[3] ? new Date(invoiceRow[3]).toISOString().split('T')[0] : "",
                CURRENCYCODE: invoiceRow[4] ? invoiceRow[4] : "",
                TXD_CRDR: invoiceRow[8] ? invoiceRow[8] : "",
                CUSTOMER_CODE: invoiceRow[0] ? invoiceRow[0] : "",
                INVOICE_AMOUNT_FC: invoiceRow[6] ? invoiceRow[6] : "00.00",
                EXCHANGE_RATE: invoiceRow[5] ? invoiceRow[5] : "00.00",
                INVOICE_AMOUNT_INR: invoiceRow[7] ? invoiceRow[7] : "00.00",
                OUTSTANDING_FC: invoiceRow[10] ? invoiceRow[10] : "00.00",
                OUTSTANDING_INR: invoiceRow[9] ? invoiceRow[9] : "00.00",
            };
        });
        
        let invoiceMasterJson = JSON.stringify(invoiceJsonResult);
        await setInvoiceMasterData(invoiceMasterJson); // Store the product master data
    } catch (err) {
        console.error('Error executing the customer master query:', err);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Error executing the customer master query, "+err, false);
        throw err; // Rethrow the error to handle it in the calling function
    }
}

// *** Start Salesforce API Integrations ***

// Function to log in to Salesforce and get the access token
async function loginToSalesforce() {
    const config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: 'https://shubhadapolymers--shudev24.sandbox.my.salesforce.com/services/oauth2/token',
        params: {
            grant_type: 'password',
            client_id: 'Paste client Id Here',
            client_secret: 'Paster client secret here',
            username: 'Salesforce user name here',
            password: 'sales force password',
        },
        headers: {}
    };

    try {
        const response = await axios.request(config);
        globalAccessToken = response.data.access_token; // Store the access token globally
        return response.data; // Return the access token data
    } catch (error) {
        console.error('Error logging in to Salesforce:', error);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: For salesforce Authentications API, "+error.response.data.messsage, false);
        throw error; // Rethrow error for further handling
    }
}

// Function to create a product master in Salesforce using the JSON Data
async function setproductMasterData(productData) {
    if (!globalAccessToken) {
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Access token is not available. Please login first ", false);
        throw new Error('Access token is not available. Please login first.');
    }

    const config = {
        method: 'put',
        maxBodyLength: Infinity,
        url: 'https://shubhadapolymers--shudev24.sandbox.my.salesforce.com/services/apexrest/ProductEntry',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${globalAccessToken}`
        },
        data: productData
    };
    const now = new Date();
    try {
        await axios.request(config).then((response) => {
            console.log("Product master :: "+JSON.stringify(response.data))
            if (response.status == 200) {
                console.log("Product Master Sales Force API hit successfully");
                sendLogUpdates("Date : " + now.toString() + " Success Log :: Product master data send ", true);
            }
            else {
                sendLogUpdates("Date : " + now.toString() + " Error Log :: " + response.data.message, false);
            }
            return response.data; // Return the response data
        })
        .catch((err) => {
            console.log('Error creating product:'+err);
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Product master Salesforce API " + err.response.data.message, false);
        });

    } catch (error) {
        console.log('Error creating product:', error);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Product master Salesforce API," + error.response.data.message, false);
        
    }
}

// function to set a customer master in salesforce using the JSON Data
async function setCustomerMasterData(customerData) {
    if (!globalAccessToken) {
        sendLogUpdates("Date : " + now.toString() + " Access token expired :: " + err, false);
        throw new Error('Access token is not available. Please login first.');
    }

    let config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: 'https://shubhadapolymers--shudev24.sandbox.my.salesforce.com/services/apexrest/AccountEntry',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${globalAccessToken}`
        },
        data: customerData
    };

    axios.request(config)
        .then((response) => {
            console.log("Account : "+JSON.stringify(response.data))
            if(response.data.status == "Inserted/Updated Account Successfully"){
                sendLogUpdates("Date : " + now.toString() + " Success Log :: Inserted/Updated Account Successfully", false);
            }
            else{
                sendLogUpdates("Date : " + now.toString() + " Error Log :: Failed to Insert/Update Account Data :: Error Description - "+response.data.message, false);
            }
        })
        .catch((error) => {
            console.log("Account errro :: "+error)
            if(error.response && error.response.data.message){
                sendLogUpdates("Date : " + now.toString() + " Error Log :: For account master salesforce API " +error.response.data.message, false);
            }
            
        });

}

// function to set a Sales Order in salesforce using the JSON Data
async function setSalesOrderData(orderData) {
    if (!globalAccessToken) {
        sendLogUpdates("Date : " + now.toString() + " Access token expired :: " + err, false);
        throw new Error('Access token is not available. Please login first.');
    }

    let config = {
        method: 'put',
        maxBodyLength: Infinity,
        url: 'https://shubhadapolymers--shudev24.sandbox.my.salesforce.com/services/apexrest/OrderEntry',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${globalAccessToken}`
        },
        data: orderData
    };

    await axios.request(config)
        .then((response) => {
            console.log("Order Data ::"+JSON.stringify(response.data))
            if(response.data.status == "200"){
                sendLogUpdates("Date : " + now.toString() + " Success Log :: Inserted/Updated Orders Data Successfully", false);
            }
            else{
                sendLogUpdates("Date : " + now.toString() + " Error Log :: Failed to Insert/Update Orders Data Data :: Error Description - "+response.data.message, false);
            }
        })
        .catch((error) => {
            console.log("order errro :: "+error)
            if(error.response && error.response.data.message){
                sendLogUpdates("Date : " + now.toString() + " Error Log :: For Sales order salesforce API " +error.response.data.message, false);
            }
            
        });

}


// function to set a Invoice Master in salesforce using the JSON Data
async function setInvoiceMasterData(invoiceData) {
    if (!globalAccessToken) {
        sendLogUpdates("Date : " + now.toString() + " Access token expired :: " + err, false);
        throw new Error('Access token is not available. Please login first.');
    }

    let config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: 'https://shubhadapolymers--shudev24.sandbox.my.salesforce.com/services/apexrest/Invoice',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${globalAccessToken}`
        },
        data: invoiceData
    };

    axios.request(config)
        .then((response) => {
            console.log("Invocie Dat ::"+JSON.stringify(response.data))
            if(response.data.status == "Inserted/Updated Invoice Data Successfully"){
                sendLogUpdates("Date : " + now.toString() + " Success Log :: Inserted/Updated Invoice Data Successfully", false);
            }
            else{
                sendLogUpdates("Date : " + now.toString() + " Error Log :: Failed to Insert/Update Invoice Data :: Error Description - "+response.data.message, false);
            }
        })
        .catch((error) => {
            console.log("Invoice errro :: "+error)
            if(error.response && error.response.data.message){
                sendLogUpdates("Date : " + now.toString() + " Error Log :: For Invoice salesforce API " +error.response.data.message, false);
            }
            
        });
}

// *** END Salesforce API Integrations ***

// Generate Log function
function sendLogUpdates(taskName, shouldSucceed) {
    if (shouldSucceed) {
        logger.logSuccess(`${taskName}`);
    } else {
        logger.logError(`${taskName} `);
    }
}

// Main function to call the function of product master and salesforce API execution
async function main() {
    let connection;
    const now = new Date();

    try {
        connection = await getDbConnection(); // Establish the connection

        // Execute the query only if the connection is active
        if (isConnectionActive(connection)) {
            const tokenData = await loginToSalesforce();
            console.log("Token Data : "+JSON.stringify(tokenData));
            // Use the tokenData for further API requests to Salesforce
            sendLogUpdates("Date : " + now.toString() + " success Log :: Token Result "+JSON.stringify(tokenData), false);
           
            //await executeProductMaster(connection); // Execute the product master query

            //await executeCustomerMaster(connection); // Execute the customer master query

            await executeSalesOrder(connection); // Execute the Sales Order Query

            ///await  executeInvoiceMasterData(connection); // Execute The Invocie Data Query

        } else {
            console.error('Database connection is not active.');
            sendLogUpdates("Date : " + now.toString() + " Error Log :: Database connection is not active.", false);
        }
    } catch (err) {
        console.error('Error in main function:', err);
        sendLogUpdates("Date : " + now.toString() + " Error Log :: Error in main function," + err, false);
    } finally {
        if (connection) {
            try {
                await connection.close(); // Always close the connection
                console.log('Database connection closed.');
            } catch (err) {
                console.error('Error closing the connection:', err);
                sendLogUpdates("Date : " + now.toString() + " Error Log :: Error closing the connection:", err, false);
            }
        }
    }
}

cron.schedule('*/10 * * * *', () => {
    console.log("Date :: "+new Date()+" ::: Main Function Called")
    main();
});
  

main(); // Run the main function
