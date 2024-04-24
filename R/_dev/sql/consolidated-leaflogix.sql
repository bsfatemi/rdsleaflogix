
---------------- STOCK LAB RESULTS -----------------

CREATE TABLE
    "consolidated"."leaflogix".stock_lab_results
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                 character(30) NOT NULL,
        store               character(30) NOT NULL,
        lastModifiedDateUtc timestamp with time zone NOT NULL,
        productId           character(100) NOT NULL,
        sku                 character(100) NOT NULL,
        packageId           character(100) NOT NULL,
        sampleDate          timestamp with time zone,
        labTestStatus       varchar,
        labTest             character(100),
        labValue            numeric,
        labResultUnitId     integer,
        labResultUnit       character,
        PRIMARY KEY (org_uuid, store_uuid, productId, sku, packageId, labTest)
    );

---------------- STOCK SNAPSHOT -----------------

CREATE TABLE
    "consolidated"."leaflogix".stock_snapshot
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                   character NOT NULL,
        store                 character NOT NULL,
        lastModifiedDateUtc   timestamp with time zone NOT NULL,
        productId             character NOT NULL,
        categoryId            character,
        batchId               character,
        packageId             character NOT NULL,
        strainId              character,
        vendorId              character,
        brandId               character,
        externalPackageId     character,
        producerId            character,
        sku                   character NOT NULL,
        unitWeightUnit        character,
        unitCost              numeric,
        allocatedQuantity     numeric,
        productName           varchar,
        category              varchar,
        imageUrl              varchar,
        quantityAvailable     numeric,
        quantityUnits         character,
        unitWeight            numeric,
        flowerEquivalent      numeric,
        recFlowerEquivalent   numeric,
        flowerEquivalentUnits character,
        batchName             character,
        packageStatus         character,
        unitPrice             numeric,
        medUnitPrice          numeric,
        recUnitPrice          numeric,
        strain                varchar,
        strainType            character,
        size                  character,
        testedDate            timestamp with time zone,
        sampleDate            timestamp with time zone,
        packagedDate          timestamp with time zone,
        manufacturingDate     timestamp with time zone,
        vendor                varchar,
        expirationDate        timestamp with time zone,
        pricingTierName       varchar,
        brandName             character,
        medicalOnly           boolean,
        producer              character,
        potencyIndicator      character,
        masterCategory        character,
        effectivePotencyMg    numeric,
        isCannabis            boolean,
        packageNDC            varchar,
        PRIMARY KEY (org_uuid, store_uuid, productId, packageId, sku)
    );

ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "org" TYPE character(30);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "store" TYPE character(30);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "productid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "categoryid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "batchid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "packageid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "strainid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "vendorid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "brandid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "externalpackageid" TYPE character
    (100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "producerid" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "sku" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "unitweightunit" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "quantityunits" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "flowerequivalentunits" TYPE
    character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "batchname" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "packagestatus" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "straintype" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "size" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "brandname" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "producer" TYPE character(100);
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "potencyindicator" TYPE character(100
    );
ALTER TABLE
    "consolidated"."leaflogix"."stock_snapshot" ALTER COLUMN "mastercategory" TYPE character(100)

---------------- STOCK BY ROOM -----------------

CREATE TABLE
    "consolidated"."leaflogix".stock_by_room
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org               character(30) NOT NULL,
        store             character(30) NOT NULL,
        lastModifiedUtc   timestamp with time zone NOT NULL,
        productId         character(50) NOT NULL,
        sku               character(50) NOT NULL,
        packageId         character(50) NOT NULL,
        roomId            character(50) NOT NULL,
        room              varchar,
        quantityAvailable numeric,
        PRIMARY KEY (org_uuid, store_uuid, productId, sku, packageId, roomId)
    );



---------------- CUSTOMER SUMMARY -----------------


CREATE TABLE
    "consolidated"."leaflogix".customer_summary
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                           character(30) NOT NULL,
        store                         character(30) NOT NULL,
        customerId                    character(100) NOT NULL,
        lastModifiedDateUTC           timestamp with time zone NOT NULL,
        creationDate                  timestamp with time zone,
        externalCustomerId            character(100),
        mergedIntoCustomerId          character(100),
        name                          varchar,
        firstName                     character(50),
        lastName                      character(50),
        middleName                    character(50),
        nameSuffix                    character(50),
        namePrefix                    character(50),
        address1                      varchar,
        address2                      varchar,
        city                          character(100),
        state                         character(50),
        postalCode                    character(50),
        phone                         character(20),
        cellPhone                     character(20),
        emailAddress                  character(100),
        status                        character(100),
        mmjidNumber                   character(100),
        mmjidExpirationDate           date,
        customerType                  character(50),
        gender                        character(50),
        driversLicenseHash            varchar,
        dateOfBirth                   date,
        createdByIntegrator           character(100),
        isAnonymous                   boolean,
        referralSource                character(100),
        otherReferralSource           character(100),
        springBigMemberId             character(100),
        customIdentifier              character(100),
        createdAtLocation             varchar,
        notes                         varchar,
        isLoyaltyMember               boolean,
        primaryQualifyingCondition    varchar,
        secondaryQualifyingConditions varchar,
        discountGroups                varchar
        PRIMARY KEY (org_uuid, store_uuid, customerId)
    );


---------------- CUSTOMER LOYALTY -----------------

CREATE TABLE
    "consolidated"."leaflogix".customer_loyalty
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org            character(30) NOT NULL,
        store          character(30) NOT NULL,
        customerId     character(100) NOT NULL,
        loyaltyBalance numeric,
        loyaltySpent   numeric,
        loyaltyEarned  numeric,
        PRIMARY KEY (org_uuid, store_uuid, customerId)
    );





---------------- STORE EMPLOYEES -----------------


CREATE TABLE
    "consolidated"."leaflogix".store_employees
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                 character(30) NOT NULL,
        store               character(30) NOT NULL,
        userId              character(100) NOT NULL,
        loginId             character(100) NOT NULL,
        fullName            character(100),
        defaultLocation     varchar,
        status              character(100),
        stateId             character(100),
        mmjExpiration       date,
        permissionsLocation varchar,
        groups              varchar,
        PRIMARY KEY (org_uuid, store_uuid, userId)
    );


---------------- ORDER ITEMS -----------------

    CREATE TABLE
    "consolidated"."leaflogix".order_items
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                     character(30) NOT NULL,
        store                   character(30) NOT NULL,
        transactionId           character(100) NOT NULL,
        transactionItemId       character(100) NOT NULL,
        productId               character(100) NOT NULL,
        sourcePackageId         character(100),
        totalPrice              numeric,
        quantity                numeric,
        unitPrice               numeric,
        unitCost                numeric,
        packageId               character(100) NOT NULL,
        totalDiscount           numeric,
        unitId                  character(100),
        unitWeight              numeric,
        unitWeightUnit          character(100),
        flowerEquivalent        numeric,
        flowerEquivalentUnit    character(100),
        returnDate              timestamp with time zone,
        isReturned              boolean,
        returnedByTransactionId character(100),
        returnReason            varchar,
        batchName               character(100),
        vendor                  varchar,
        isCoupon                boolean,
        order_item_discount_json json,
        order_item_tax_json json,
        PRIMARY KEY (org_uuid, store_uuid, transactionId, transactionItemId, productId, packageId)
    );


---------------- ORDER SUMMARY -----------------


CREATE TABLE
    "consolidated"."leaflogix".order_summary
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                        character NOT NULL,
        store                      character NOT NULL,
        transactionId              character NOT NULL,
        customerId                 character NOT NULL,
        employeeId                 character NOT NULL,
        transactionDate            timestamp with time zone NOT NULL,
        voidDate                   timestamp with time zone,
        isVoid                     boolean,
        subtotal                   numeric,
        totalDiscount              numeric,
        totalBeforeTax             numeric,
        tax                        numeric,
        tipAmount                  numeric,
        total                      numeric,
        paid                       numeric,
        changeDue                  numeric,
        totalItems                 integer,
        terminalName               character,
        checkInDate                timestamp with time zone,
        invoiceNumber              integer,
        isTaxInclusive             boolean,
        transactionType            character,
        loyaltyEarned              numeric,
        loyaltySpent               numeric,
        lastModifiedDateUTC        timestamp with time zone,
        cashPaid                   numeric,
        debitPaid                  numeric,
        electronicPaid             numeric,
        electronicPaymentMethod    character,
        checkPaid                  numeric,
        creditPaid                 numeric,
        giftPaid                   numeric,
        mmapPaid                   numeric,
        prePaymentAmount           numeric,
        revenueFeesAndDonations    numeric,
        nonRevenueFeesAndDonations numeric,
        returnOnTransactionId      character,
        adjustmentForTransactionId character,
        orderType                  character,
        wasPreOrdered              boolean,
        orderSource                character,
        orderMethod                character,
        invoiceName                character,
        isReturn                   boolean,
        authCode                   character,
        customerTypeId             character,
        isMedical                  boolean,
        totalCredit                numeric,
        completedByUser            character,
        transactionDateLocalTime   timestamp with time zone,
        estTimeArrivalLocal        timestamp with time zone,
        estDeliveryDateLocal       timestamp with time zone,
        feesAndDonations_json json,
        orderIds_json json,
        order_discount_json json,
        order_tax_json json,
        PRIMARY KEY (org_uuid, store_uuid, transactionId, customerId, employeeId, transactionDate)
    );



---------------- BRAND INDEX -----------------


CREATE TABLE
    "consolidated"."leaflogix".brand_index
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org       character(30) NOT NULL,
        store     character(30) NOT NULL,
        brandId   character(100) NOT NULL,
        brandName varchar NOT NULL,
        PRIMARY KEY (org_uuid, store_uuid, brandId)
    );



---------------- PRODUCT SUMMARY -----------------

CREATE TABLE
    "consolidated"."leaflogix".product_summary
    (
        org_uuid uuid NOT NULL,
        store_uuid uuid NOT NULL,
        org                           character NOT NULL,
        store                         character NOT NULL,
        productId                     character NOT NULL,
        sku                           character NOT NULL,
        internalName                  character,
        masterCategory                character,
        categoryId                    character,
        category                      character,
        imageUrl                      varchar,
        strainId                      character,
        strain                        character,
        strainType                    character,
        size                          character,
        netWeight                     numeric,
        netWeightUnitId               character,
        netWeightUnit                 character,
        brandId                       character,
        brandName                     character,
        vendorId                      character,
        vendorName                    character,
        isCannabis                    boolean,
        isActive                      boolean,
        isCoupon                      boolean,
        thcContent                    numeric,
        thcContentUnit                character,
        cbdContent                    numeric,
        cbdContentUnit                character,
        productGrams                  numeric,
        flowerEquivalent              numeric,
        recFlowerEquivalent           numeric,
        price                         numeric,
        medPrice                      numeric,
        recPrice                      numeric,
        unitCost                      numeric,
        unitType                      character,
        onlineTitle                   varchar,
        onlineDescription             varchar,
        onlineProduct                 boolean,
        posProducts                   boolean,
        pricingTier                   integer,
        onlineAvailable               boolean,
        lowInventoryThreshold         numeric,
        pricingTierName               varchar,
        pricingTierDescription        varchar,
        flavor                        character,
        alternateName                 character,
        lineageName                   character,
        distillationName              character,
        maxPurchaseablePerTransaction numeric,
        dosage                        character,
        instructions                  varchar,
        allergens                     varchar,
        defaultUnit                   varchar,
        producerId                    character,
        producerName                  character,
        createdDate                   timestamp with time zone,
        isMedicalOnly                 boolean,
        lastModifiedDateUTC           timestamp with time zone,
        grossWeight                   numeric,
        isTaxable                     boolean,
        upc                           character,
        regulatoryCategory            character,
        ndc                           character,
        daysSupply                    integer,
        illinoisTaxCategory           character,
        externalCategory              character,
        syncExternally                boolean,
        regulatoryName                character,
        administrationMethod          character,
        unitCBDContentDose            character,
        unitTHCContentDose            character,
        oilVolume                     character,
        ingredientList                varchar,
        expirationDays                integer,
        abbreviation                  character,
        isTestProduct                 boolean,
        isFinished                    boolean,
        allowAutomaticDiscounts       boolean,
        servingSize                   character,
        servingSizePerUnit            boolean,
        isNutrient                    boolean,
        productName                   varchar,
        description                   varchar,
        imageUrls_json json,
        taxCategories_json json,
        pricingTierData_json json,
        PRIMARY KEY (org_uuid, store_uuid, productId, sku)
    );
