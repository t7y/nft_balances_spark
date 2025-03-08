from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    expr,
    substring,
    lit,
    sum as spark_sum,
    posexplode,
    element_at,
    udf
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json
import os


def create_spark_session():
    """Create and return a SparkSession with the proper configuration."""
    spark = SparkSession.builder \
        .appName("NFTTransfers") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    return spark


def write_schemas(output_dir='output/schemas'):
    """Write JSON schema files for documentation/validation."""
    LOG_SCHEMA_JSON = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Log Entry",
        "type": "object",
        "properties": {
            "address": {"type": "string", "description": "Address from which the log originated"},
            "topics": {"type": "array", "description": "List of log topics", "items": {"type": "string"}},
            "data": {"type": "string", "description": "Data associated with the log"},
            "blockNumber": {"type": "string", "description": "Block number in hex"},
            "transactionHash": {"type": "string", "description": "Transaction hash that generated the log entry"},
            "transactionIndex": {"type": "string", "description": "Transaction index within the block"},
            "blockHash": {"type": "string", "description": "Block hash in which the log was included"},
            "logIndex": {"type": "string", "description": "Index in the block representing the log order"}
        },
        "required": ["address", "topics", "data", "blockNumber", "transactionHash", "transactionIndex", "blockHash",
                     "logIndex"]
    }

    TRANSACTION_SCHEMA_JSON = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Transaction",
        "type": "object",
        "properties": {
            "hash": {"type": "string", "description": "Transaction hash"},
            "blockHash": {"type": "string", "description": "Hash of the block containing this transaction"},
            "yParity": {"type": "string", "description": "Y parity indicator for EIP-1559 transactions"},
            "accessList": {"type": "array", "description": "Access list for the transaction",
                           "items": {"type": "object"}},
            "transactionIndex": {"type": "string", "description": "The transaction's index within the block"},
            "type": {"type": "string", "description": "Transaction type"},
            "nonce": {"type": "string", "description": "Transaction nonce"},
            "input": {"type": "string", "description": "Transaction input data"},
            "r": {"type": "string", "description": "Signature parameter r"},
            "s": {"type": "string", "description": "Signature parameter s"},
            "chainId": {"type": "string", "description": "Chain ID"},
            "v": {"type": "string", "description": "Signature parameter v"},
            "blockNumber": {"type": "string", "description": "Block number in hex (should match the block's number)"}
        },
        "required": [
            "hash", "blockHash", "yParity", "accessList", "transactionIndex",
            "type", "nonce", "input", "r", "s", "chainId", "v", "blockNumber"
        ]
    }

    BLOCK_SCHEMA_JSON = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Block",
        "type": "object",
        "properties": {
            "number": {"type": "string", "description": "Block number in hex"},
            "hash": {"type": "string", "description": "Block hash"},
            "parentHash": {"type": "string", "description": "Parent block hash"},
            "timestamp": {"type": "string", "description": "Block timestamp in hex"},
            "transactions": {
                "type": "array",
                "description": "List of transactions included in the block",
                "items": {"$ref": "transaction_schema.json"}
            }
        },
        "required": ["number", "hash", "parentHash", "timestamp", "transactions"]
    }

    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, 'log_schema.json'), 'w', encoding='utf-8') as f:
        json.dump(LOG_SCHEMA_JSON, f, indent=2)
    with open(os.path.join(output_dir, 'transaction_schema.json'), 'w', encoding='utf-8') as f:
        json.dump(TRANSACTION_SCHEMA_JSON, f, indent=2)
    with open(os.path.join(output_dir, 'block_schema.json'), 'w', encoding='utf-8') as f:
        json.dump(BLOCK_SCHEMA_JSON, f, indent=2)


def define_spark_schema():
    """Define and return the Spark schema for a block (for reference/documentation)."""
    log_schema = StructType([
        StructField("address", StringType(), True),
        StructField("topics", ArrayType(StringType()), True),
        StructField("data", StringType(), True),
        StructField("removed", StringType(), True)
    ])

    transaction_schema = StructType([
        StructField("hash", StringType(), True),
        StructField("logs", ArrayType(log_schema), True)
    ])

    block_schema = StructType([
        StructField("number", StringType(), True),
        StructField("hash", StringType(), True),
        StructField("transactions", ArrayType(transaction_schema), True)
    ])
    return block_schema


def read_and_parse_json(spark, json_path):
    """
    Read the JSON file and return a DataFrame of blocks.
    The file is assumed to be a JSON array.
    """
    # Enable multiLine parsing so that the entire JSON array is parsed correctly.
    df = spark.read.option("multiLine", "true").json(json_path)
    return df


def explode_logs(df):
    """Explode the transactions and logs so each row represents a single log event."""
    df_tx = df.select("number", "hash", "parentHash", "timestamp", "transactions") \
        .withColumn("tx", explode(col("transactions")))
    return df_tx.withColumn("log", explode(col("tx.logs")))


def process_erc721(df_logs):
    """Process ERC-721 transfer events and return incoming/outgoing DataFrames with NFT type and contract address."""
    ERC721_TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    df_nft721 = df_logs.filter(expr(f"array_contains(log.topics, '{ERC721_TRANSFER_SIG}')"))
    df_nft721_extracted = df_nft721.select(
        col("log.address").alias("contract_address"),
        substring(col("log.topics")[1], 27, 40).alias("from_address"),
        substring(col("log.topics")[2], 27, 40).alias("to_address"),
        col("log.topics")[3].alias("token_id")
    )
    # Add NFT type column
    df_nft721_extracted = df_nft721_extracted.withColumn("nft_type", lit("ERC721"))

    df_incoming = df_nft721_extracted.select(
        col("to_address").alias("wallet"),
        "token_id", "contract_address", "nft_type"
    ).withColumn("delta", lit(1)).withColumn("amount", lit(1))

    df_outgoing = df_nft721_extracted.select(
        col("from_address").alias("wallet"),
        "token_id", "contract_address", "nft_type"
    ).withColumn("delta", lit(-1)).withColumn("amount", lit(1))

    return df_incoming, df_outgoing


def process_erc1155_single(df_logs):
    """Process ERC-1155 TransferSingle events and return incoming/outgoing DataFrames with NFT type and contract address."""
    ERC1155_TRANSFER_SINGLE_SIG = "0xc3d58168c5b9b7aefa1e5b2baf53f96b18c0d04ad89d4e6a58d1c97"
    df_single = df_logs.filter(expr(f"array_contains(log.topics, '{ERC1155_TRANSFER_SINGLE_SIG}')"))
    df_extracted = df_single.select(
        col("log.address").alias("contract_address"),
        substring(col("log.topics")[2], 27, 40).alias("from_address"),
        substring(col("log.topics")[3], 27, 40).alias("to_address"),
        substring(col("log.data"), 3, 64).alias("token_id"),
        substring(col("log.data"), 67, 64).alias("amount_hex")
    )
    df_extracted = df_extracted.withColumn("amount", expr("conv(amount_hex, 16, 10)"))
    # Add NFT type column
    df_extracted = df_extracted.withColumn("nft_type", lit("ERC1155"))

    df_incoming = df_extracted.select(
        col("to_address").alias("wallet"),
        "token_id", "contract_address", "nft_type", "amount"
    ).withColumn("delta", col("amount"))

    df_outgoing = df_extracted.select(
        col("from_address").alias("wallet"),
        "token_id", "contract_address", "nft_type", "amount"
    ).withColumn("delta", -col("amount"))

    return df_incoming, df_outgoing


def process_erc1155_batch(df_logs):
    """Process ERC-1155 TransferBatch events and return incoming/outgoing DataFrames with NFT type and contract address."""
    ERC1155_TRANSFER_BATCH_SIG = "0x4a39dc06d4c0dbc64b70b5e7f3c8b"  # Adjust full value if needed.
    df_batch = df_logs.filter(expr(f"array_contains(log.topics, '{ERC1155_TRANSFER_BATCH_SIG}')"))

    def decode_transfer_batch(data):
        try:
            hex_str = data[2:]  # Remove '0x'
            n = int(hex_str[128:192], 16)
            token_ids = []
            pos = 192
            for _ in range(n):
                token_ids.append(hex_str[pos:pos + 64])
                pos += 64
            m = int(hex_str[pos:pos + 64], 16)
            pos += 64
            amounts = []
            for _ in range(m):
                amounts.append(hex_str[pos:pos + 64])
                pos += 64
            return (token_ids, amounts)
        except Exception as e:
            return ([], [])

    batch_schema = StructType([
        StructField("token_ids", ArrayType(StringType()), True),
        StructField("amounts", ArrayType(StringType()), True)
    ])

    def decode_batch(data):
        token_ids, amounts = decode_transfer_batch(data)
        return {"token_ids": token_ids, "amounts": amounts}

    decode_batch_udf = udf(decode_batch, batch_schema)
    df_decoded = df_batch.withColumn("decoded", decode_batch_udf(col("log.data")))
    df_exploded = df_decoded.select(
        col("log.address").alias("contract_address"),
        substring(col("log.topics")[2], 27, 40).alias("from_address"),
        substring(col("log.topics")[3], 27, 40).alias("to_address"),
        col("decoded.token_ids").alias("token_ids"),
        col("decoded.amounts").alias("amounts")
    ).selectExpr(
        "contract_address",
        "from_address",
        "to_address",
        "posexplode(token_ids) as (pos, token_id)",
        "amounts"
    )
    df_extracted = df_exploded.withColumn(
        "amount_hex", element_at(col("amounts"), col("pos") + 1)
    ).withColumn("amount", expr("conv(amount_hex, 16, 10)"))
    # Add NFT type column
    df_extracted = df_extracted.withColumn("nft_type", lit("ERC1155"))

    df_incoming = df_extracted.select(
        col("to_address").alias("wallet"),
        "token_id", "contract_address", "nft_type", "amount"
    ).withColumn("delta", col("amount"))

    df_outgoing = df_extracted.select(
        col("from_address").alias("wallet"),
        "token_id", "contract_address", "nft_type", "amount"
    ).withColumn("delta", -col("amount"))

    return df_incoming, df_outgoing


def union_transfers(dfs):
    """Union a list of DataFrames."""
    unioned = dfs[0]
    for df in dfs[1:]:
        unioned = unioned.union(df)
    return unioned


def main():
    # Set input and output paths
    json_path = "./input/gnosis_30747660.json"
    output_path = "./output/"

    spark = create_spark_session()
    write_schemas()  # Optional: write JSON schema files for reference

    # Read and parse the JSON file (with multiLine enabled)
    df_parsed = read_and_parse_json(spark, json_path)
    # Explode transactions and logs
    df_logs = explode_logs(df_parsed)

    # Process transfers for ERC-721, ERC-1155 TransferSingle, and ERC-1155 TransferBatch
    erc721_in, erc721_out = process_erc721(df_logs)
    erc1155_single_in, erc1155_single_out = process_erc1155_single(df_logs)
    erc1155_batch_in, erc1155_batch_out = process_erc1155_batch(df_logs)

    # Union all incoming and outgoing transfers.
    df_incoming = union_transfers([erc721_in, erc1155_single_in, erc1155_batch_in])
    df_outgoing = union_transfers([erc721_out, erc1155_single_out, erc1155_batch_out])
    df_transfers = df_incoming.union(df_outgoing)

    # Compute net balance per wallet, token_id, contract_address, and nft_type.
    nft_balances = df_transfers.groupBy("wallet", "token_id", "contract_address", "nft_type").agg(
        spark_sum("delta").alias("balance")
    )
    nft_balances.show(truncate=False)

    # Write the balances as Parquet without overwriting the folder (using append mode)
    nft_balances.write.mode("append").parquet(output_path)

    spark.stop()


if __name__ == '__main__':
    main()