import logging
import pandas as pd
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
   StructType,
   StructField,
   StringType,
   IntegerType,
   DoubleType,
   BinaryType,
)
from mtaairflowlib.utils.models import Zone
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator
from sedona.utils.adapter import Adapter
from sedona.core.formatMapper.shapefileParser import ShapefileReader

# Define some global vars

allowed_fr = {
   "B",
   "F",
   "FWD",
   "1",
}  # allowed direction codes for forward travel (REF->NREF)
allowed_to = {
   "B",
   "T",
   "REV",
   "2",
}  # allowed direction codes for reverse travel (NREF->REF)

cols_to_attach = [
   "REF_IN_ID",
   "NREF_IN_ID",
   "LINK_ID",
   "ST_NAME",
   "ST_NM_PREF",
   "ST_TYP_BEF",
   "ST_NM_BASE",
   "ST_NM_SUFF",
   "ST_TYP_AFT",
   "DIR_TRAVEL",
   "BRIDGE",
   "FUNC_CLASS",
   "SPEED_CAT",
   "TUNNEL",
   "RAMP",
   "TOLLWAY",
   "POIACCESS",
   "CONTRACC",
   "ROUNDABOUT",
   "TO_LANES",
   "FROM_LANES",
   "geometry",
   "LANE_CAT",
   "EXPR_LANE",
   "PHYS_LANES",
   "LOW_MBLTY",
   "PRIORITYRD",
   "L_POSTCODE",
   "R_POSTCODE",
]  # These are the columns we are interested in

ROUTE_SCHEMA = StructType(
   [  # declares the schema for the output of the pandas UDF
       StructField("name_norm", StringType(), False),
       StructField("dir_code", StringType(), False),
       StructField("chain_id", StringType(), False),
       StructField("seq", IntegerType(), False),
       StructField("LINK_ID", StringType(), False),
       StructField("from_node", StringType(), False),
       StructField("to_node", StringType(), False),
       StructField("length_ft", DoubleType(), True),
       StructField("start_lat", DoubleType(), True),
       StructField("start_lon", DoubleType(), True),
       StructField("end_lat", DoubleType(), True),
       StructField("end_lon", DoubleType(), True),
       StructField("bearing_deg", DoubleType(), True),
       StructField("geometry_wkb", BinaryType(), True),
   ]
)


def hash_chain_key(parts):
   return hashlib.sha256("-".join(parts).encode("utf-8")).hexdigest()


def ingest_raw_from_dir(spark, input_path: str) -> DataFrame:
   spatial_rdd = ShapefileReader.readToGeometryRDD(spark.sparkContext, input_path)
   return Adapter.toDf(spatial_rdd, spark)


# output is a single spark dataframe with all raw shapefiles combined


def normalize_schema_crs(
   sdf: DataFrame,
   *,
   geom_col: str = "geometry",
   assume_srid: int = 4326,  # assume 4326 (lat/lon) if not specified
) -> (
   DataFrame
):  # define task that reads the previous dataframe and applies the function

   # quick check to make sure geometry is present
   if geom_col not in sdf.columns:
       raise ValueError(f"Normalize_schema_crs missing geometry column: '{geom_col}'")

   # select relevant attributes
   select_exprs = []
   for c in cols_to_attach:
       if c in sdf.columns:
           select_exprs.append(F.col(c))
       else:
           # keep schema stable if some columns are missing
           select_exprs.append(
               F.lit(None).cast("string").alias(c)
           )  # cast missing columns as null strings
   links = sdf.select(
       *select_exprs, F.col(geom_col).alias("geom")
   )  # select only relevant columns + geometry

   # set SRID and transform to EPSG: 4326
   links = links.selectExpr(
       "*", f"ST_SetSRID(geom, {assume_srid}) as geom_srid"
   )  # assume input geometry is in assume_srid, set the SRID accordingly
   links = links.selectExpr(
       "*", "ST_Transform (geom_srid, 'epsg:4326') as geometry"
   ).drop(
       "geom", "geom_srid"
   )  # transform to 4326 and drop temporary geometry columns

   # keep non-empty linestrings
   links = links.where(
       F.expr(
           "ST_GeometryType(geometry) rlike 'LineString$' and NOT ST_IsEmpty(geometry)"
       )
   )  # does geometry type match what we expect? And drop empty geometries

   # normalize DT and name_norm
   links = links.withColumn(
       "DT", F.upper(F.trim(F.col("DIR_TRAVEL")))
   )  # creates normalized version of direction code

   links = links.withColumn(
       "name_norm", F.expr("regexp_replace(upper(coalesce(ST_NAME,'')),'\\\\s+','')")
   )  # creates normalized version of street name (upper case, no spaces, treat nulls as empty string)

   # cast IDs to string
   links = (
       links.withColumn(
           "REF_IN_ID", F.col("REF_IN_ID").cast("string")
       )  # cast all as string to avoid mismatching
       .withColumn("NREF_IN_ID", F.col("NREF_IN_ID").cast("string"))
       .withColumn("LINK_ID", F.col("LINK_ID").cast("string"))
   )

   return links


# output is same as previous, with normalized schema and updated dtypes


def compute_lengths(
   links: DataFrame,
   *,
   length_crs: int = 6539,  # default is epsg 6539, which is ft-based for NY/NJ area
   replace_geometry: bool = False,
) -> DataFrame:

   out = links.withColumn(
       "length_ft",
       F.expr(
           f"ST_Length(ST_Transform(geometry, 'epsg:{length_crs}'))"
       ),  # reproject to length_crs and compute length in feet
   ).withColumn(
       "length_miles", F.col("length_ft") / F.lit(5280.0)  # calculate miles from feet
   )
   if replace_geometry:
       out = out.withColumn(
           "geometry",
           F.expr(
               f"ST_Transform(geometry, 'epsg:4326')"
           ),  # reproject back to 4326 for downstream use
       )

   return out


# output is the same as previous function, but with lengths computed


def build_chains_per_group(pdf: pd.DataFrame) -> pd.DataFrame:
   # Empty guard
   if pdf.empty:
       return pd.DataFrame(columns=[f.name for f in ROUTE_SCHEMA.fields])

   # Normalize column types
   for c in ["from_node", "to_node", "LINK_ID", "next_link"]:
       if c in pdf.columns:
           pdf[c] = pdf[c].astype("string")

   # Build quick lookups
   info_map = {
       r.LINK_ID: (
           r.from_node,
           r.to_node,
           r.length_ft,
           r.dir_code,
           r.start_lat,
           r.start_lon,
           r.end_lat,
           r.end_lon,
           r.bearing_deg,
           r.geometry_wkb,
       )  # LINK_ID -> (from_node, to_node, length_ft, dir_code)
       for r in pdf.itertuples(index=False)
   }
   next_map = {r.LINK_ID: r.next_link for r in pdf.itertuples(index=False)}

   nl = pdf["next_link"].astype("string")
   has_pred = set(nl[nl.notna() & (nl.str.strip() != "")].str.strip().unique())

   links = pdf["LINK_ID"].astype("string")
   seeds = [lid for lid in links.tolist() if lid not in has_pred]

   if not seeds:
       seeds = [links.min()]

   out_rows = []
   used = set()

   for start in seeds:  # iterate through links identified as seeds
       if start in used:  # skip if we already used this seed
           continue

       chain = []
       lid = start
       max_steps = len(pdf)
       steps = 0

       # Follow the single successor pointer until it ends (or hits a cycle)
       while (
           lid is not None and lid not in chain and steps < max_steps
       ):  # while we have more link ids that
           chain.append(lid)  # append current link to chain
           nx = next_map.get(lid)  # get this link's successor from next_map
           if pd.notna(nx):
               nx = str(nx).strip()
           lid = (
               nx if (pd.notna(nx) and nx != "") else None
           )  # if there's a valid successor, continue. If not, stop.
           steps += 1

       first_from, *_ = info_map[
           chain[0]
       ]  # get from_node and dir_code of first link in chain
       chain_key = [str(first_from)] + [str(x) for x in chain]  # build chain key
       chain_id = hash_chain_key(chain_key)

       for i, l in enumerate(chain, start=1):  # enumerate links in chain
           (
               from_node,
               to_node,
               length_ft,
               link_dir,
               start_lat,
               start_lon,
               end_lat,
               end_lon,
               bearing_deg,
               geometry_wkb,
           ) = info_map[
               l
           ]  # get to_node and length_ft of current link
           out_rows.append(
               (
                   pdf["name_norm"].iat[0],
                   link_dir,  # per-link direction
                   chain_id,
                   i,
                   l,
                   from_node,
                   to_node,
                   float(length_ft) if length_ft is not None else None,
                   float(start_lat) if start_lat is not None else None,
                   float(start_lon) if start_lon is not None else None,
                   float(end_lat) if end_lat is not None else None,
                   float(end_lon) if end_lon is not None else None,
                   float(bearing_deg) if bearing_deg is not None else None,
                   geometry_wkb,
               )
           )

       used.update(
           chain
       )  # guard in case somehow an interior link went into seeds- it stops us from rebuilding the same chain starting from that link

   return pd.DataFrame(out_rows, columns=[f.name for f in ROUTE_SCHEMA.fields])


def build_directed_chains(
   links_len: DataFrame,
   *,
   allowed_fr={"B", "F", "FWD", "1"},
   allowed_to={"B", "T", "REV", "2"},
   bearing_max_deg: float = 45.0,  # maximum turn angle to consider a valid successor
   prefer_same_funcclass: bool = True,
) -> DataFrame:
   # 1) Build directed edges
   edges_f = links_len.where(F.col("DT").isin(list(allowed_fr))).select(
       "LINK_ID",
       "name_norm",
       "length_ft",
       "FUNC_CLASS",
       F.col("REF_IN_ID").alias("from_node"),
       F.col("NREF_IN_ID").alias("to_node"),
       F.lit("F").alias("dir_code"),
       "geometry",
   )
   edges_r = links_len.where(F.col("DT").isin(list(allowed_to))).select(
       "LINK_ID",
       "name_norm",
       "length_ft",
       "FUNC_CLASS",
       F.col("NREF_IN_ID").alias("from_node"),
       F.col("REF_IN_ID").alias("to_node"),
       F.lit("T").alias("dir_code"),
       "geometry",
   )
   edges = edges_f.unionByName(edges_r)

   # 2) Compute bearing of each directed edge geometry. We use this as a tie-breaker for potential chains and also use to calculate cardinal direction
   oriented = edges.withColumn(
       "geom_oriented",
       F.when(F.col("dir_code") == "T", F.expr("ST_Reverse(geometry)")).otherwise(
           F.col("geometry")
       ),
   )
   oriented = (
       oriented.withColumn("sp", F.expr("ST_StartPoint(geom_oriented)"))
       .withColumn("ep", F.expr("ST_EndPoint(geom_oriented)"))
       .withColumn("start_lon", F.expr("ST_X(sp)"))
       .withColumn("start_lat", F.expr("ST_Y(sp)"))
       .withColumn("end_lon", F.expr("ST_X(ep)"))
       .withColumn("end_lat", F.expr("ST_Y(ep)"))
       .drop("sp", "ep")
   )

   lat1 = F.radians(F.col("start_lat").cast("double"))
   lat2 = F.radians(F.col("end_lat").cast("double"))
   dlon = F.radians(F.col("end_lon").cast("double")) - F.radians(
       F.col("start_lon").cast("double")
   )
   x = F.sin(dlon) * F.cos(lat2)
   y = F.cos(lat1) * F.sin(lat2) - F.sin(lat1) * F.cos(lat2) * F.cos(dlon)
   bearing = F.pmod(F.degrees(F.atan2(x, y)) + F.lit(360.0), F.lit(360.0))
   oriented = oriented.withColumn("bearing_deg", bearing)

   # 3) Successor candidates (a -> b) on same street + node continuity
   cand = (
       oriented.alias("a")
       .join(
           oriented.alias("b"),
           (F.col("a.to_node") == F.col("b.from_node"))
           & (F.col("a.name_norm") == F.col("b.name_norm"))
           & (F.col("a.LINK_ID") != F.col("b.LINK_ID"))
           & (F.col("a.from_node") != F.col("b.to_node")),
           how="left",
       )
       .select(
           F.col("a.name_norm").alias("name_norm"),
           F.col("a.dir_code").alias("dir_code"),
           F.col("a.LINK_ID").alias("a_LINK_ID"),
           F.col("a.from_node").alias("a_from"),
           F.col("a.to_node").alias("a_to"),
           F.col("a.length_ft").alias("a_len"),
           F.col("a.bearing_deg").alias("a_bearing"),
           F.col("a.FUNC_CLASS").alias("a_fc"),
           F.col("a.geom_oriented").alias(
               "a_geometry"
           ),  # make sure we have directional endpoints
           F.col("a.start_lat").alias("a_start_lat"),
           F.col("a.start_lon").alias("a_start_lon"),
           F.col("a.end_lat").alias("a_end_lat"),
           F.col("a.end_lon").alias("a_end_lon"),
           F.expr("ST_AsBinary(a.geom_oriented)").alias("a_geometry_wkb"),
           F.col("b.LINK_ID").alias("b_LINK_ID"),
           F.col("b.length_ft").alias("b_len"),
           F.col("b.bearing_deg").alias("b_bearing"),
           F.col("b.FUNC_CLASS").alias("b_fc"),
       )
   )

   # 4) Turn angle + pick best successor
   angle = F.abs((F.col("b_bearing") - F.col("a_bearing") + 540.0) % 360.0 - 180.0)
   cand = cand.withColumn("turn_angle", angle)

   # boolean: prefer same functional class first
   same_fc_col = F.when(
       F.lit(prefer_same_funcclass) & (F.col("a_fc") == F.col("b_fc")), 1
   ).otherwise(0)
   cand = cand.withColumn("same_fc", same_fc_col)
   # keep only reasonable turns
   is_unnamed = F.col("name_norm") == ""

   cand = cand.where(
       F.col("b_LINK_ID").isNull()
       | (~is_unnamed)
       | (F.col("turn_angle") <= F.lit(bearing_max_deg))
   )

   # order: same_fc desc (1 before 0), then smallest turn, then optional stable tie-breakers
   w = W.partitionBy("name_norm", "dir_code", "a_LINK_ID").orderBy(
       F.col("b_LINK_ID").isNull().asc(),
       F.col("same_fc").desc(),
       F.col("turn_angle").asc(),
       F.col("b_len").desc(),  # optional: prefer longer successor
       F.col("b_LINK_ID").asc(),
   )  # optional: deterministic tie-break

   adj = (
       cand.withColumn("rn", F.row_number().over(w))
       .where(F.col("rn") == 1)
       .select(
           "name_norm",
           "dir_code",
           F.col("a_LINK_ID").alias("LINK_ID"),
           F.col("a_from").alias("from_node"),
           F.col("a_to").alias("to_node"),
           F.col("a_len").alias("length_ft"),
           F.col("a_bearing").alias("bearing_deg"),
           F.col("a_geometry").alias("geometry"),
           F.col("a_start_lat").alias("start_lat"),
           F.col("a_start_lon").alias("start_lon"),
           F.col("a_end_lat").alias("end_lat"),
           F.col("a_end_lon").alias("end_lon"),
           F.col("a_geometry_wkb").alias("geometry_wkb"),
           F.col("b_LINK_ID").cast("string").alias("next_link"),
       )
   )

   # 6) Build chains per (street,dir) using the simplified UDF above
   routes = adj.groupBy("name_norm").applyInPandas(
       build_chains_per_group, schema=ROUTE_SCHEMA
   )

   # 7) Normalize sequence
   wseq = W.partitionBy("name_norm", "chain_id").orderBy("seq", "LINK_ID")
   routes_spark = (
       routes.withColumn("seq", F.row_number().over(wseq))
       .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry_wkb)"))
       .drop("geometry_wkb")
   )

   return routes_spark


def get_cardinal_bearings(  # functon to get cardinal direction
   df: DataFrame,
) -> DataFrame:
   return df.withColumn(
       "dir_cardinal",
       F.when((F.col("bearing_deg") >= 45) & (F.col("bearing_deg") < 135), "E")
       .when((F.col("bearing_deg") >= 135) & (F.col("bearing_deg") < 225), "S")
       .when((F.col("bearing_deg") >= 225) & (F.col("bearing_deg") < 315), "W")
       .otherwise(F.lit("N")),
   )


EXPLICIT_RENAMES = {
   "LINK_ID": "link_id",
   "REF_IN_ID": "reference_node_id",
   "NREF_IN_ID": "nref_node_id",
   "dir_code": "direction_code",
   "LINK-DIR": "link_direction",
   "ROUTE_name": "route_name",
   "name_norm": "street_name_norm",
   "from_node": "from_node_id",
   "to_node": "to_node_id",
   "chain_id": "chain_id",
   "seq": "sequence",
   "ST_NAME": "street_name",
   "ST_NM_PREF": "street_name_prefix",
   "ST_TYP_BEF": "street_type_before",
   "ST_NM_BASE": "street_name_base",
   "ST_NM_SUFF": "street_name_suffix",
   "ST_TYP_AFT": "street_type_after",
   "geometry": "geometry",
   "start_lat": "start_lat",
   "start_lon": "start_lon",
   "end_lat": "end_lat",
   "end_lon": "end_lon",
   "bearing_deg": "bearing_degrees",
   "dir_cardinal": "cardinal_direction",
   "length_ft": "length_feet",
   "length_miles": "length_miles",
   "TO_LANES": "to_lanes",
   "FROM_LANES": "from_lanes",
   "LANE_CAT": "lane_category",
   "PHYS_LANES": "physical_lanes",
   "EXPR_LANE": "express_lane",
   "LOW_MBLTY": "low_mobility",
   "PRIORITYRD": "priority_road",
   "FUNC_CLASS": "functional_class",
   "SPEED_CAT": "speed_category",
   "BRIDGE": "bridge",
   "TUNNEL": "tunnel",
   "RAMP": "ramp",
   "TOLLWAY": "tollway",
   "POIACCESS": "poi_access",
   "CONTRACC": "controlled_access",
   "ROUNDABOUT": "roundabout",
   "L_POSTCODE": "left_postcode",
   "R_POSTCODE": "right_postcode",
}


def apply_explicit_renames(df: DataFrame, mapping: dict) -> DataFrame:
   return df.select([F.col(c).alias(mapping.get(c, c)) for c in df.columns])


def get_args():
   p = ArgumentParser()
   p.add_argument(
       "--log_lvl",
       choices=["DEBUG", "INFO", "WARN", "ERROR"],
       default="INFO",
       type=str.upper,
       help="Logging level (default: %(default)s)",
   )
   p.add_argument("--source_zone", type=Zone.from_json, required=True)
   p.add_argument("--target_zone", type=Zone.from_json, required=True)
   p.add_argument("--target", required=True)
   p.add_argument("--length_crs", type=int, default=6539)

   return p.parse_args()


def main(args, logger):
   spark = (
       SparkSession.builder.config(
           "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
       )
       .config("spark.kryo.registrator", SedonaKryoRegistrator.getName())
       .config("spark.sql.adaptive.enabled", "true")
       .getOrCreate()
   )
   SedonaRegistrator.registerAll(spark)  # register sedona functions

   # 1) read raw shapefiles
   input_dir = args.source_zone.get_table_path("here")
   logger.info(
       f"reading raw HERE shapefiles from zone = {args.source_zone} path={input_dir}"
   )
   links_raw_sdf = ingest_raw_from_dir(spark, input_dir)
   logger.info(f"Raw rows: {links_raw_sdf.count()}")

   # 2) Normalize Schema
   links_norm = normalize_schema_crs(links_raw_sdf)
   logger.info(
       f"after normalizing: rows = {links_norm.count()} (EPSG:4326, geometry kept as-is, dropped empties)"
   )

   # 3) Compute Lengths
   links_len = compute_lengths(links_norm, length_crs=args.length_crs)
   logger.info("Computed Lengths in Feet and Miles")

   # 4) Create Forward and Reverse Sequences per Street
   routes_spark = build_directed_chains(
       links_len,
       allowed_fr=allowed_fr,
       allowed_to=allowed_to,
       bearing_max_deg=45.0,
       prefer_same_funcclass=True,
   )
   logger.info(f"Chains built: rows = {routes_spark.count()}")

   # 5) Add Link-Dir and cardinal direction
   final_sdf = get_cardinal_bearings(
       routes_spark.withColumn(
           "LINK-DIR", F.col("LINK_ID").cast("string") + F.col("dir_code")
       ).withColumn("ROUTE_name", F.col("name_norm"))
   )

   # quick debug to check for mixed directions in a single chain
   mixed_dir_debug = (
       final_sdf.groupBy("chain_id")
       .agg(F.countDistinct("dir_code").alias("dirs"))
       .where(F.col("dirs") > 1)
       .count()
   )
   logger.info(f"Chains with multiple direction codes: {mixed_dir_debug}")

   # 6) Normalize column names and format
   final_named = apply_explicit_renames(final_sdf, EXPLICIT_RENAMES)

   # 7) Reorder Columns
   front = [
       "link_direction",
       "chain_id",
       "link_id",
       "route_name",
       "direction_code",
       "sequence",
       "from_node_id",
       "to_node_id",
       "start_lat",
       "start_lon",
       "end_lat",
       "end_lon",
   ]
   existing_first = [c for c in front if c in final_named.columns]
   rest = [c for c in final_named.columns if c not in existing_first]
   final_out = final_named.select(*(existing_first + rest))

   # 8) Write Delta
   target_path = args.target_zone.get_table_path(args.target)
   (
       final_out.write.format("delta")
       .option("path", target_path)
       .option("overwriteSchema", "true")
       .mode("overwrite")
       .saveAsTable(args.target)
   )
   logger.info(f"Wrote Delta to {target_path}")


def setup_logging(lvl: str = "INFO"):
   log_format = "%(asctime)-15s [%(levelname)s] %(name)s.%(funcName)s():: %(message)s"
   logging.basicConfig(
       level=lvl,
       format=log_format,
       datefmt="%Y-%m-%d %H:%M:%S %Z",
   )


if __name__ == "__main__":
   args = get_args()
   setup_logging(args.log_lvl)
   logger = logging.getLogger("dim_here_road_links")
   logger.info("Beginning Execution")
   main(args, logger)
   logger.info("Execution Complete")













