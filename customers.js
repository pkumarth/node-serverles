import log from '../../../lib/logger';
import service from './customerService'
const {
    getCustomerByCustomerId,
    updateBusinessPartnerStatusAndRetryCount,
    updateBusinessPartnerStatusToRetry,
    sendSqsMessage,
    createReprocessCustomerResponse,
    getCustomersBySyncStatus,
    processCustomers,
    generateCustomerReprocessingStatus,
} = service;


// Function to handle the main flow of reprocessing a failed customer
async function postReprocessFailedCustomerByCustomerIdHandler(businessPartnerId, log, event) {
    const {body,query,params,header} = req;
    const businessPartnerId = params.customerId;
    log.info({
        message: 'Reprocess Failed Customer By CustomerID Request',
        marker: 'REQUEST-INBOUND',
        type: 'BEFORE_REQUEST',
        request: businessPartnerId,
        response: ''
    });

    const customerDetails = await getCustomerByCustomerId(businessPartnerId);

    if (customerDetails.length > 0) {
        const retryCount = customerDetails[0].wrappedBusinessPartner.envelope.businessPartnerEnvelope.retryCount + 1;
        const failCount = customerDetails[0].wrappedBusinessPartner.envelope.businessPartnerEnvelope.failCount;
        const idocNo = customerDetails[0].wrappedBusinessPartner.envelope.businessPartnerEnvelope.idocNumber;

        const businessPartnerSyncStatus = process.env.REPROCESS_RETRY_LIMIT < retryCount ? 'ECC-BP-RETRY' : 'BP-CREATE-FAILED';

        if (businessPartnerSyncStatus === 'BP-CREATE-FAILED') {
            await updateBusinessPartnerStatusAndRetryCount(businessPartnerId, retryCount);
        } else {
            await updateBusinessPartnerStatusToRetry(businessPartnerId);
            await sendSqsMessage(customerDetails);
        }
    }

    log.info({
        message: 'Reprocess Failed Customer By CustomerId Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: event,
        response: ''
    });

    return createReprocessCustomerResponse(businessPartnerId);
}
const postReprocessAllFailedCustomersHandler =async(req,res)=> {
    const {body,query,params,header} = req;
    const customerId=params.customerId
    log.info({
        message: 'Reprocess All Failed Customers Request',
        marker: 'REQUEST-INBOUND',
        type: 'BEFORE_REQUEST',
        request: customerId,
        response: ''
    });
    const businessPartnerSyncStatus = 'BP-CREATE-FAILED';
    const reprocessLimit = parseInt(process.env.REPROCESS_FAILED_CUSTOMERS_LIMIT);

    const customers = await getCustomersBySyncStatus(businessPartnerSyncStatus, reprocessLimit);

    let result;
    if (customers.length > 0) {
        const businessPartnerIds = customers.map(c => c.wrappedBusinessPartner.envelope.businessPartnerEnvelope.businessPartnerId).join(', ');
        result = await processCustomers(customers, sqsQueueDetails);
    } else {
        result = {
            httpStatus: 204
        };
    }
    log.info({
        message: 'Reprocess All Failed Customers Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: customerId,
        response: result
    });
    const response = generateCustomerReprocessingStatus(result);

    return response;
}





export default {
    postReprocessAllFailedCustomersHandler,
    postReprocessFailedCustomerByCustomerIdHandler
};
// service
import utils from './customerTransform'
const {
    getWrappedBusinessPartnerSyncStatus,
    createBusinessPartnerQuery,
    createBusinessPartnerSyncStatusPayload,
    extractBusinessPartnerSyncInfo,
    prepareSqsMessagePayload, 
    
    createWrappedBusinessPartnerEnvelope,
    wrapBusinessPartnerId,
    updateBusinessPartnerSyncStatusAndRetryCount,
    updateBusinessPartnerEnvelopeStatus,
    prepareSqsMessageWithAttributes
} = utils;
// Function to get customer details by customerId from MongoDB
const getCustomerByCustomerId = async(businessPartnerId)=> {
    log.info({
        message: 'Get Customer By CustomerId Request',
        marker: 'REQUEST-OUTBOUND',
        type: 'BEFORE_REQUEST',
        request: businessPartnerId,
        response: ''
    });

    const query = createWrappedBusinessPartnerEnvelope(businessPartnerId);
    const result = await findDocumentInMongo(collection,query);

    log.info({
        message: 'Get Customer By CustomerId Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: businessPartnerId,
        response: result
    });

    return result;
}
// Function to update business partner status and retry count
const updateBusinessPartnerStatusAndRetryCount= async(businessPartnerId, retryCount)=> {
    const query = wrapBusinessPartnerId(businessPartnerId);
    const update = updateBusinessPartnerSyncStatusAndRetryCount(retryCount);
    await collection.updateOne(query, update);
}
// Function to update business partner status to retry
const updateBusinessPartnerStatusToRetry=async(businessPartnerId)=> {
    const query = createWrappedBusinessPartnerEnvelope(businessPartnerId);
    const update = updateBusinessPartnerEnvelopeStatus();
    await collection.updateOne(query, update);
}
// Function to publish message in sqs
const sendSqsMessage=async(customerDetails)=> {
    const extractedDetails = extractBusinessPartnerDetails(customerDetails[0]);
    const sqsMessage = prepareSqsMessageWithAttributes(extractedDetails);
    const params = {
        MessageBody:sqsMessage,
        QueueUrl: queueurl
    };
    await sqs.sendMessage(params).promise();
}

// Function to retrieve customers by sync status
const getCustomersBySyncStatus = async(syncStatus, limit)=> {
    const query = getWrappedBusinessPartnerSyncStatus(syncStatus);
    const failedCount = await collection.countDocuments(query);
    const limit = limit || failedCount;
    const customers = await findDocumentWithLimitInDocDb(collection,query,limit);
    return customers;
}

// Function to process a single customer
const processCustomers= async(customer, sqsQueueDetails)=> {
    const reqPayload = customer;
    const retryCount = customer.wrappedBusinessPartner.envelope.businessPartnerEnvelope.retryCount + 1;
    const failCount = customer.wrappedBusinessPartner.envelope.businessPartnerEnvelope.failCount;
    const idocNo = customer.wrappedBusinessPartner.envelope.businessPartnerEnvelope.idocNumber;

   
    // update customer document in MongoDB
    const query = createBusinessPartnerQuery(customer);
    const update = createBusinessPartnerSyncStatusPayload(customer, retryCount);
    await collection.updateOne(query, update);

    const businessPartnerSyncInfo = extractBusinessPartnerSyncInfo(customer);
    // send message to sqs for down service
    const sqsMessagePayload = prepareSqsMessagePayload(businessPartnerSyncInfo);
    const params = {
        MessageBody: JSON.stringify(sqsMessagePayload),
        QueueUrl: queueurl
    };
    await sqs.sendMessage(params).promise();
}


const generateCustomerReprocessingStatus = (businessPartnerIds, payload) => {
    const result = {
        customerIds: businessPartnerIds,
        reprocessCount: payload?.length ?? 0,
        status: "Success",
        message: payload?.length > 0 ? "Re-processing all failed Customers." : "No failed Customers to re-process."
    };
    return result;
};

const createReprocessCustomerResponse = (businessPartnerId, reprocessCount) => {
    const response = {
        customerId: businessPartnerId,
        reprocessCount: reprocessCount,
        status: "Success",
        message: "Re-process failed Customer"
    };

    return response;
};


export default {
    getCustomerByCustomerId,
    updateBusinessPartnerStatusAndRetryCount,
    updateBusinessPartnerStatusToRetry,
    sendSqsMessage,
    createReprocessCustomerResponse,
    getCustomersBySyncStatus,
    processCustomers,
    generateCustomerReprocessingStatus, 
};

// transformer

const extractOrderDetails = (reqPayload, orderSyncStatus) => {
    const result = {
        orderId: reqPayload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.orderId,
        idocNumber: reqPayload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.idocNumber,
        orderType: reqPayload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.orderType,
        orderSyncStatus: orderSyncStatus
    };

    return result;
};

const updateOrderSyncStatusAndRetryCount = (orderSyncStatus, retryCount) => {
    const result = {
        "wrappedSkuOrder": {
            "envelope": {
                "skuOrderEnvelope": {
                    "orderSyncStatus": orderSyncStatus,
                    "retryCount": retryCount
                }
            }
        }
    };
    return result;
};

const updateOrderSyncStatus = (orderSyncStatus) => {
    const result = {
        "wrappedSkuOrder": {
            "envelope": {
                "skuOrderEnvelope": {
                    "orderSyncStatus": orderSyncStatus
                }
            }
        }
    };
    return result;
};

const generateFailedSkuOrderQuery = (gte, lte) => {
    const query = {
        $and: [{
            "'wrappedSkuOrder.envelope.skuOrderEnvelope.lastFailedAttemptTimeDate'": {
                $gte: `ISODate('${gte}')`,
                $lt: `ISODate('${lte}')`
            }
        }, {
            "'wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus'": "'SOM-ORD-CREATE-FAILED'"
        }]
    };

    return JSON.stringify(query).replace(/=/g, ":");
};

const getWrappedSkuOrderSyncStatus = (orderSyncStatus) => {
    const result = {
        "wrappedSkuOrder": {
            "envelope": {
                "skuOrderEnvelope": {
                    "orderSyncStatus": orderSyncStatus
                }
            }
        }
    };
    return result;
};

const prepareOrderMessageForSQS = (payload, reqPayload, zindicator, IdocNo, correlationId) => {
    const orderId = reqPayload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.orderId;

    const message = {
        delaySeconds: 0,
        body: JSON.stringify(payload),
        deduplicationId: zindicator ? `${orderId}-${zindicator || ''}` : orderId,
        groupId: "som-order",
        messageAttributes: {
            "x-B3-SpanId": {
                stringValue: correlationId,
                dataType: "String.x-B3-SpanId"
            },
            "x-B3-TraceId": {
                stringValue: IdocNo,
                dataType: "String.x-B3-TraceId"
            },
            "x-client-service-moniker": {
                stringValue: 'MULE_PROPERTY_app.service-moniker',
                dataType: "String.x-client-service-moniker"
            }
        }
    };

    return message;
};

const buildOrderAndIdocQuery = (payload) => {
    return {
        "$and": [{
            "wrappedSkuOrder.envelope.skuOrderEnvelope.orderId": payload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.orderId
        }, {
            "wrappedSkuOrder.envelope.skuOrderEnvelope.idocNumber": payload?.wrappedSkuOrder?.envelope?.skuOrderEnvelope?.idocNumber
        }]
    };
};

const generateFailedOrderFilterQuery = (queryParams) => {
    if (queryParams?.['filter[orderId]'] != null) {
        return {
            "$and": [{
                "wrappedSkuOrder.envelope.skuOrderEnvelope.orderId": queryParams['filter[orderId]']
            }, {
                "wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus": "SOM-ORD-CREATE-FAILED"
            }]
        };
    } else if (queryParams?.['filter[customerNumber]'] != null) {
        return {
            "$and": [{
                "wrappedSkuOrder.envelope.skuOrderEnvelope.customerNumber": queryParams['filter[customerNumber]']
            }, {
                "wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus": "SOM-ORD-CREATE-FAILED"
            }]
        };
    } else if (queryParams?.['filter[poNumber]'] != null) {
        return {
            "$and": [{
                "wrappedSkuOrder.envelope.skuOrderEnvelope.poNumber": queryParams['filter[poNumber]']
            }, {
                "wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus": "SOM-ORD-CREATE-FAILED"
            }]
        };
    } else if (queryParams?.['filter[timeofOrderCreation]'] != null) {
        return {
            "$and": [{
                "wrappedSkuOrder.envelope.skuOrderEnvelope.timeOfOrderCreation": queryParams['filter[timeofOrderCreation]']
            }, {
                "wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus": "SOM-ORD-CREATE-FAILED"
            }]
        };
    } else {
        return '';
    }
};

const buildFailedOrderQueryFilter = (vars) => {
    const dateFilter = (() => {
        if (vars.toDate && vars.fromDate) {
            return {
                $gte: `ISODate('${vars.fromDate}')`,
                $lt: `ISODate('${vars.toDate}')`
            };
        } else if (vars.fromDate) {
            return {
                $gte: `ISODate('${vars.fromDate}')`
            };
        } else if (vars.toDate) {
            return {
                $lt: `ISODate('${vars.toDate}')`
            };
        } else {
            return {};
        }
    })();

    let query;
    if (Object.keys(dateFilter).length > 0 && vars.errorCode) {
        query = {
            $and: [{
                'wrappedSkuOrder.envelope.skuOrderEnvelope.lastFailedAttemptTimeDate': dateFilter
            }, {
                'wrappedSkuOrder.envelope.skuOrderEnvelope.errorCode': vars.errorCode
            }, {
                'wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus': 'SOM-ORD-CREATE-FAILED'
            }]
        };
    } else if (Object.keys(dateFilter).length > 0) {
        query = {
            $and: [{
                'wrappedSkuOrder.envelope.skuOrderEnvelope.lastFailedAttemptTimeDate': dateFilter
            }, {
                'wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus': 'SOM-ORD-CREATE-FAILED'
            }]
        };
    } else if (vars.errorCode) {
        query = {
            $and: [{
                'wrappedSkuOrder.envelope.skuOrderEnvelope.errorCode': vars.errorCode
            }, {
                'wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus': 'SOM-ORD-CREATE-FAILED'
            }]
        };
    } else {
        query = {
            'wrappedSkuOrder.envelope.skuOrderEnvelope.orderSyncStatus': 'SOM-ORD-CREATE-FAILED'
        };
    }

    return JSON.parse(JSON.stringify(query).replace(/"/g, "'"));
};

export default {
	extractOrderDetails,
	updateOrderSyncStatusAndRetryCount,
	updateOrderSyncStatusAndRetryCount,
	updateOrderSyncStatus,
	generateFailedSkuOrderQuery,
	buildOrderAndIdocQuery,
	getWrappedSkuOrderSyncStatus,
	prepareOrderMessageForSQS,
	buildOrderAndIdocQuery,
	generateFailedOrderFilterQuery,
	buildFailedOrderQueryFilter
};

