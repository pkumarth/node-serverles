import log from '../../../lib/logger';
import service from './orderService'
const {
    getFailedOrdersByParams,
    getLastFailedAttemptTimedate,
    getFailedOrdersById,
    getFailedOrdersFromQueryParams,
    getOrdersBySyncStatus,
    sendSqsMessage,
    processFailedOrders,
    generateOrderReprocessingStatus
} = service;

// Function to handle the main flow of reprocessing all failed orders
const postReprocessAllFailedOrders=async(req,res)=> {
    const {body,query,params,header} = req;
    const orderId= query.params.orderId;
    log.info({
        message: 'Reprocess All Failed Orders Request',
        marker: 'REQUEST-OUTBOUND',
        type: 'BEFORE_REQUEST',
        request: orderId,
        response: ''
    });

    const orderSyncStatus = "SOM-ORD-CREATE-FAILED";
    const reprocessLimit = parseInt(process.env.REPROCESS_FAILED_ORDERS_LIMIT);

    const failedOrders = await getFailedOrdersByParams(query, orderSyncStatus, reprocessLimit);

    if (failedOrders.length > 0) {
        const orderIDs = failedOrders.map(order => order.orderId).join(',');
        await processFailedOrders(failedOrders, sqsQueueDetails);
    }

    const response = generateOrderReprocessingStatus(failedOrders);

    log.info({
        message: 'Reprocess All Failed Orders Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: event,
        response
    });

    return response;
}

export default {
    postReprocessAllFailedOrders,
};
//---
import utils from './orderTransform'
const {
    extractOrderDetails,
    updateOrderSyncStatusAndRetryCount,
    updateOrderSyncStatus,
    generateFailedSkuOrderQuery,
    getWrappedSkuOrderSyncStatus,
    prepareOrderMessageForSQS,
    buildOrderAndIdocQuery,
    generateFailedOrderFilterQuery,
    buildFailedOrderQueryFilter
} = utils;


const collection = 'som-orders';
const projection = {
    wrappedSkuOrder: 1
};

// Function to retrieve failed orders based on various parameters
const getFailedOrdersByParams = async (queryParams, orderSyncStatus, reprocessLimit) => {
    log.info({
        message: 'Get Failed Orders Request',
        marker: 'REQUEST-OUTBOUND',
        type: 'BEFORE_REQUEST',
        request: queryParams,
        response: ''
    });

    let failedOrders;
    if (query) {
        if (queryParams['filter[lastFailedAttemptTimeDate]']) {
            failedOrders = await getLastFailedAttemptTimedate(queryParams);
        } else if (queryParams['filter[orderId]'] || queryParams['filter[customerNumber]'] || queryParams['filter[poNumber]'] || queryParams['filter[timeofOrderCreation]']) {
            failedOrders = await getFailedOrdersById(queryParams);
        } else if (queryParams.errorCode || queryParams.fromDate || queryParams.toDate) {
            failedOrders = await getFailedOrdersFromQueryParams(queryParams, reprocessLimit);
        }
    }
    else {
        failedOrders = await getOrdersBySyncStatus(orderSyncStatus, reprocessLimit);
    }

    const transformedFailedOrders = failedOrders.length > 0 ? failedOrders.map(order => order.wrappedSkuOrder.envelope) : [];

    log.info({
        message: 'Get Failed Orders Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: queryParams,
        response: transformedFailedOrders
    });

    return transformedFailedOrders;
}

// Function to get failed orders based on last failed attempt time
const getLastFailedAttemptTimedate = async (queryParams) => {
    const [gte, lte] = queryParams['filter[lastFailedAttemptTimeDate]'].split('..');
    const query = generateFailedSkuOrderQuery(gte, lte);
    const result = findDocument(collection, query,projection);
    return result;
}

// Function to get failed orders by ID
const getFailedOrdersById = async (queryParams) => {
    log.info({
        message: 'Get Failed Orders By Id Request',
        marker: 'REQUEST-OUTBOUND',
        type: 'BEFORE_REQUEST',
        request: queryParams,
        response: ''
    });
    const query = generateFailedOrderFilterQuery(queryParams);
    const result = findDocument(collection, query, projection);
    log.info({
        message: 'Get Failed Orders By Id Response',
        marker: 'RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: queryParams,
        response: result
    });
    return result;
}
// Function to get failed orders from query parameters
const getFailedOrdersFromQueryParams=async(queryParams, reprocessLimit)=> {
    const {
        errorCode,
        fromDate,
        toDate
    } = queryParams;
    const query = buildFailedOrderQueryFilter(errorCode, fromDate, toDate);
    const failedCount = await countDocuments(collection,query);
    const limit = reprocessLimit || failedCount;
    const result = await findDocumentsWithLimit(collection,query,limit,projection);
    return result;

}

// Function to get orders by sync status
const getOrdersBySyncStatus = async (orderSyncStatus, reprocessLimit) => {
    const query = getWrappedSkuOrderSyncStatus(orderSyncStatus);
    const failedCount = await countDocuments(collection, query);
    const limit = reprocessLimit || failedCount;
    const result = await findDocumentsWithLimit(collection, query, limit, projection)
    return result;
}
// Function to publish order message in sqs
const sendSqsMessage=async(order)=> {
    const orderDetails = extractOrderDetails(order);
    const zindicator = orderDetails.ZORDERS05.IDOC.E1EDK01.ZE1EDK02A.find(item => item.ZINDICATOR)?.ZINDICATOR;

    const sqsMessage = prepareOrderMessageForSQS(orderDetails, zindicator);

    log.info({
        message: 'Reprocess All Failed Orders SQS Request payload',
        marker: 'SQS-REQUEST',
        type: 'AFTER_REQUEST',
        request: sqsMessage,
        response: ''
    });
    
    const params = {
        MessageBody:sqsMessage,
        QueueUrl: queueurl
    };
    await sqs.sendMessage(params).promise();

    log.info({
        message: 'Reprocess All Failed Orders SQS Response payload',
        marker: 'SQS-RESPONSE-OUTBOUND',
        type: 'AFTER_REQUEST',
        request: sqsMessage,
        response: ''
    });
}
// Function to process failed orders
const processFailedOrders=async(failedOrders)=> {
    for (const order of failedOrders) {
        const retryCount = (order.wrappedSkuOrder.envelope.skuOrderEnvelope.retryCount || 0) + 1;
        const failCount = order.wrappedSkuOrder.envelope.skuOrderEnvelope.failCount || 0;
        const idocNo = order.wrappedSkuOrder.envelope.skuOrderEnvelope.idocNumber;

        const orderSyncStatus = parseInt(process.env.REPROCESS_RETRY_LIMIT) < retryCount ? "ECC-ORD-RETRY" : "SOM-ORD-CREATE-FAILED";
        if (orderSyncStatus === "SOM-ORD-CREATE-FAILED") {
            const query = buildOrderAndIdocQuery(order.orderId, idocNo);
            const update = updateOrderSyncStatusAndRetryCount(orderSyncStatus, retryCount, failCount);
            await collection.updateOne(query, update);
        } else {
            const query = buildOrderAndIdocQuery(order.orderId, idocNo);
            const update = updateOrderSyncStatusAndRetryCount(orderSyncStatus, retryCount, failCount);
            await collection.updateOne(query, update);
            sendSqsMessage(order)

        }
    }
}

const generateOrderReprocessingStatus = (orderIDs, payload) => {
    const result = {
        orderIds: orderIDs,
        reprocessCount: payload?.length ?? 0,
        status: "Success",
        message: payload?.length > 0 ? "Re-processing all failed orders." : "No failed orders to re-process."
    };

    return removeNullValues(result);
};

export default {
    getFailedOrdersByParams,
    getLastFailedAttemptTimedate,
    getFailedOrdersById,
    getFailedOrdersFromQueryParams,
    getOrdersBySyncStatus,
    sendSqsMessage,
    processFailedOrders,
    generateOrderReprocessingStatus
};
//---

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

