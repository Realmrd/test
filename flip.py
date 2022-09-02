from shroomdk import ShroomDK

sdk = ShroomDK("7f75ae4f-fd68-4655-be41-d442d6d7c490")
my_address = lower("0x6EA012A3249ccc35D020Dd4124b739956966699e")
sql = f"""
  with base as
((with cp_owner as
(SELECT
  TOKENID,
  nt.tx_hash as tx_hash,
  case when NFT_TO_ADDRESS = '0x0000000000000000000000000000000000000000'
  then event_inputs:to else NFT_TO_ADDRESS end as NFT_TO_ADDRESS,
  nt.BLOCK_TIMESTAMP,
  row_number()over (partition by tokenid order by nt.BLOCK_TIMESTAMP DESC,nt.event_index DESC) as rn,
  row_number()over (partition by nt.tx_hash order by nt.event_index DESC) as ei1,
  case when rn = 1 then '1' else ei1 end as ei
FROM ethereum.core.ez_nft_transfers nt
  left join ethereum.core.fact_event_logs el ON nt.tx_hash = el.tx_hash and nt.nft_address = el.contract_address
where nft_address in ('0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb')
  and (el.event_name = 'Transfer' or el.event_name is null)
  and (el.contract_name = 'CryptoPunksMarket' or el.contract_name is null)
  ORDER BY 4 DESC
)


  SELECT
  tokenid as punkid,
  NFT_TO_ADDRESS as owner
  from cp_owner
where rn = 1
  	and ei = 1
	and nft_to_address != '0xb7f7f6c52f2e2fdb1963eab30438024864c313f6'
order by tokenid+0)

UNION -- for the nfts who never been transferd

(SELECT
  event_inputs:amount as punkid,
  event_inputs:to as owner
FROM ethereum.core.fact_event_logs
where event_name = 'Assign'
  and ORIGIN_FROM_ADDRESS = '0xc352b534e8b987e036a93539fd6897f53488e56a'
  and contract_address in ('0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb')
  and punkid not in (
  (with cp_owner as
(SELECT
  TOKENID,
  nt.tx_hash as tx_hash,
  case when NFT_TO_ADDRESS = '0x0000000000000000000000000000000000000000'
  then event_inputs:to else NFT_TO_ADDRESS end as NFT_TO_ADDRESS,
  nt.BLOCK_TIMESTAMP,
  row_number()over (partition by tokenid order by nt.BLOCK_TIMESTAMP DESC,nt.event_index DESC) as rn,
  row_number()over (partition by nt.tx_hash order by nt.event_index DESC) as ei1,
  case when rn = 1 then '1' else ei1 end as ei
FROM ethereum.core.ez_nft_transfers nt
  left join ethereum.core.fact_event_logs el ON nt.tx_hash = el.tx_hash and nt.nft_address = el.contract_address
where nft_address in ('0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb')
  and (el.event_name = 'Transfer' or el.event_name is null)
  and (el.contract_name = 'CryptoPunksMarket' or el.contract_name is null)
  ORDER BY 4 DESC
)


SELECT
  tokenid
from cp_owner
where rn = 1
  	and ei = 1
order by tokenid+0
  )
  )
order by punkid+0
  )

  UNION ALL -- wrapped NFTs
  
(with wpunk as
(SELECT
  tokenid,
  nft_to_address,
  row_number()over (partition by tokenid order by BLOCK_TIMESTAMP DESC,event_index DESC) as rn,
  row_number()over (partition by tx_hash order by event_index DESC) as ei1,
  case when rn = 1 then '1' else ei1 end as ei
FROM ethereum.core.ez_nft_transfers
WHERE PROJECT_NAME in ('Wrapped CryptoPunks V1','wrapped cryptopunks')
  --AND EVENT_TYPE = 'mint'
  --AND NFT_ADDRESS = '0xb7f7f6c52f2e2fdb1963eab30438024864c313f6'
  --AND NFT_FROM_ADDRESS = '0x0000000000000000000000000000000000000000'
  And tokenid in (
  WITH WRAPPEDS AS (
    SELECT TOKENID,'WRAP' TYPE, MAX(BLOCK_TIMESTAMP) DATE
    FROM ethereum.core.ez_nft_transfers
    WHERE PROJECT_NAME='wrapped cryptopunks' AND EVENT_TYPE='mint'
        AND NFT_ADDRESS='0xb7f7f6c52f2e2fdb1963eab30438024864c313f6' 
        AND NFT_FROM_ADDRESS='0x0000000000000000000000000000000000000000'
    GROUP BY 1,2
),
UNWRRAPEDS AS (
    SELECT TOKENID,'UNWRAP' TYPE, MAX(BLOCK_TIMESTAMP) LAST_UNWRAPPING_DATE 
    FROM ethereum.core.ez_nft_transfers
    WHERE PROJECT_NAME='cryptopunks'
        AND NFT_ADDRESS='0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' 
        AND NFT_FROM_ADDRESS='0xb7f7f6c52f2e2fdb1963eab30438024864c313f6'
    GROUP BY 1,2
),
FINAL AS (
    SELECT * FROM WRAPPEDS
    UNION ALL
    SELECT * FROM UNWRRAPEDS
),
CLASSIFICATED AS (
    SELECT TOKENID,TYPE,MAX(DATE) DATE
    FROM FINAL
    GROUP BY 1,2 
),
LAST_TYPE AS (
SELECT C.TOKENID,C.TYPE,C.DATE
FROM CLASSIFICATED C 
INNER JOIN 
(
    SELECT TOKENID,MAX(DATE) AS LAST_OCCURENCE
    FROM CLASSIFICATED
    GROUP BY 1
) X
ON C.TOKENID=X.TOKENID AND C.DATE=X.LAST_OCCURENCE
)
SELECT TOKENID FROM LAST_TYPE
WHERE TYPE='WRAP'
  )
)

SELECT
tokenid as punkid,
  NFT_TO_ADDRESS as owner
from wpunk
where rn = 1
  	and ei = 1
order by tokenid+0)
  
order by punkid+0)

SELECT
  *
from base
order by punkid+0
"""

results = sdk.query(sql)

query_result_set = sdk.query(sql)

for record in query_result_set.records:
    punkid = record['punkid']
    owner = record['owner']
    print(f"CP #{punkid} Owned by {owner}")
