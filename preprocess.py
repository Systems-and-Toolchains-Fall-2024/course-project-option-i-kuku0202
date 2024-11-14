import numpy as np
from pyspark.ml import Pipeline,Transformer
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Imputer,StandardScaler,StringIndexer,OneHotEncoder, VectorAssembler, IndexToString
import os
from pyspark.sql.functions import lit, monotonically_increasing_id


col_to_drop = ['sofifa_id', 'player_url', 'short_name', 'long_name', 'player_positions', 'potential', 'value_eur', 'wage_eur', 'dob', 'club_team_id', 'club_name', 'league_name', 'league_level', 'club_position', 'club_jersey_number', 'club_loaned_from', 'club_joined', 'club_contract_valid_until', 'nationality_id', 'nationality_name', 'nation_team_id', 'nation_position', 'nation_jersey_number', 'release_clause_eur', 'player_tags', 'player_traits',  'player_face_url', 'club_logo_url', 'club_flag_url', 'nation_logo_url', 'nation_flag_url', 'year', 'gender', 'unique_id', 'mentality_composure']

continuous_cols = ['overall', 'age', 'height_cm', 'weight_kg', 'weak_foot', 'skill_moves', 'international_reputation', 'pace', 'shooting', 'passing', 'dribbling', 'defending', 'physic', 'attacking_crossing', 'attacking_finishing', 'attacking_heading_accuracy', 'attacking_short_passing', 'attacking_volleys', 'skill_dribbling', 'skill_curve', 'skill_fk_accuracy', 'skill_long_passing', 'skill_ball_control', 'movement_acceleration', 'movement_sprint_speed', 'movement_agility', 'movement_reactions', 'movement_balance', 'power_shot_power', 'power_jumping', 'power_stamina', 'power_strength', 'power_long_shots', 'mentality_aggression', 'mentality_interceptions', 'mentality_positioning', 'mentality_vision', 'mentality_penalties', 'defending_marking_awareness', 'defending_standing_tackle', 'defending_sliding_tackle', 'goalkeeping_diving', 'goalkeeping_handling', 'goalkeeping_kicking', 'goalkeeping_positioning', 'goalkeeping_reflexes', 'ls', 'st', 'rs', 'lw', 'lf', 'cf', 'rf', 'rw', 'lam', 'cam', 'ram', 'lm', 'lcm', 'cm', 'rcm', 'rm', 'lwb', 'ldm', 'cdm', 'rdm', 'rwb', 'lb', 'lcb', 'cb', 'rcb', 'rb', 'gk']
nominal_cols = ['work_rate', 'body_type']
binary_cols = ['prefer_right_foot', 'real_face']


def load_player_data_from_csv(spark):
    root = 'data'
    df = spark.read.csv(os.path.join(root, 'players_15.csv'), header=True, inferSchema=True)
    schema = df.schema
    df = df.withColumn('year', lit(2015))
    df = df.withColumn('gender', lit('male'))
    for file in sorted(os.listdir(root)):
        if file.split('_')[-1][:-4] != '15':
            df = spark.read.csv(os.path.join(root, file), header=True, schema=schema)
            df = df.withColumn('year', lit(2000+int(file.split('_')[-1][:-4])))      # add new column for the year
            if file.startswith('player'):
                df = df.withColumn('gender', lit('Male'))
            else:
                df = df.withColumn('gender', lit('Female'))
            df = df.union(df)
    df = df.withColumn('unique_id', monotonically_increasing_id())    # add unique id to each row
    return df


class ColumnDropper(Transformer):
    """ Drop unnecessary columns"""
    def __init__(self, columns_to_drop = None):
        super().__init__()
        self.columns_to_drop=columns_to_drop
    def _transform(self, dataset):
        return dataset.drop(*self.columns_to_drop)
    

class Imputator(Transformer):
    def __init__(self, thresh, schema):
        super().__init__()
        self.thresh = thresh
        self.schema = schema
        self.col_heavy_missing = []
        self.impute_dict = dict()
    def _transform(self, dataset):
        df = dataset
        for col_name in df.columns:
            df_col = df.select(col_name)
            row_missing = df_col.filter(df_col[col_name].isNull()).count()
            if row_missing == 0:
                continue
            elif row_missing > self.thresh:
                self.col_heavy_missing.append(col_name)
            else:
                if (df.schema[col_name].dataType == FloatType()):
                    self.impute_dict[col_name] = df_col.filter(df_col[col_name].isNotNull()).select(mean(col_name)).collect()[0][0]
                elif (df.schema[col_name].dataType == IntegerType()):
                    self.impute_dict[col_name] = df_col.filter(df_col[col_name].isNotNull()).select(median(col_name)).collect()[0][0]
                elif (df.schema[col_name].dataType == StringType()):
                    self.impute_dict[col_name] = df_col.filter(df_col[col_name].isNotNull()).select(mode(col_name)).collect()[0][0]
                else:
                    self.impute_dict[col_name] = 0
        return df.fillna(self.impute_dict).drop(*self.col_heavy_missing)
    

class FeatureTypeCaster(Transformer):
    def __init__(self, columns):
        super().__init__()
        self.columns = columns

    def _transform(self, dataset):
        df = dataset
        for col_name in self.columns:
            if df.schema[col_name].dataType == IntegerType():
                df = df.withColumn(col_name, col(col_name).cast(FloatType()))
            elif df.schema[col_name].dataType == StringType():
                df = df.withColumn(col_name, col(col_name).cast(BooleanType()).cast(FloatType()))
        return df
    
class OutcomeCreater(Transformer):
    def __init__(self, outcome_col="overall"):
        super().__init__()
        self.outcome_col=outcome_col

    def _transform(self, dataset):
        df = dataset.withColumnRenamed(self.outcome_col, 'outcome')
        df = df.withColumn('outcome', col('outcome').cast(FloatType()))
        return df


class StringSplitter(Transformer):
    def __init__(self, ):
        self.columns = ['ls', 'st', 'rs', 'lw', 'lf', 'cf', 'rf', 'rw', 'lam', 'cam', 'ram', 'lm', 'lcm', 'cm', 'rcm', 'rm', 'lwb', 'ldm', 'cdm', 'rdm', 'rwb', 'lb', 'lcb', 'cb', 'rcb', 'rb', 'gk']
    def _transform(self, dataset):
        df = dataset
        for col_name in self.columns:
            df = df.withColumn(col_name, split(df[col_name], "\+")[0].cast(FloatType()))
        return df

    

def get_clearning_pipeline(df, col_to_drop):
    stage_col_dropper = ColumnDropper(columns_to_drop=col_to_drop)
    stage_splitter = StringSplitter()
    stage_imputator = Imputator(thresh=df.count()//5, schema=df.schema)

    pipeline = Pipeline(stages=[
        stage_col_dropper,
        stage_splitter,
        stage_imputator
    ])
    return pipeline


def get_preprocess_pipeline():
    # Stage where columns are casted as appropriate types
    stage_typecaster = FeatureTypeCaster(binary_cols+continuous_cols)

    stage_outcome = OutcomeCreater('overall')

    # Stage where nominal columns are transformed to index columns using StringIndexer
    nominal_id_cols = [x+"_index" for x in nominal_cols]
    nominal_onehot_cols = [x+"_encoded" for x in nominal_cols]
    stage_nominal_indexer = StringIndexer(inputCols = nominal_cols, outputCols = nominal_id_cols)

    # Stage where the index columns are further transformed using OneHotEncoder
    stage_nominal_onehot_encoder = OneHotEncoder(inputCols=nominal_id_cols, outputCols=nominal_onehot_cols)

    # Stage where all relevant features are assembled into a vector (and dropping a few)
    feature_cols = continuous_cols+binary_cols+nominal_onehot_cols
    feature_cols.remove('overall')
    stage_vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="vectorized_features")

    # Stage to scale the columns
    stage_scaler = StandardScaler(inputCol= 'vectorized_features', outputCol= 'features')

    # Removing all unnecessary columbs, only keeping the 'features' and 'outcome' columns
    stage_column_dropper = ColumnDropper(columns_to_drop = nominal_cols+nominal_id_cols+
        nominal_onehot_cols+ binary_cols + continuous_cols + ['vectorized_features'])
    
    # Connect the columns into a pipeline
    pipeline = Pipeline(stages=[
        stage_typecaster,
        stage_outcome,
        stage_nominal_indexer,
        stage_nominal_onehot_encoder,
        stage_vector_assembler,
        stage_scaler,
        stage_column_dropper
    ])
    return pipeline 


def clean_data(df):
    df = df[df['gender'] == 'Male']
    df = df.withColumn("prefer_right_foot", when(col("preferred_foot") == "Right", "Yes").when(col("preferred_foot") == "Left", "No")).drop('preferred_foot')
    clean_pipeline = get_clearning_pipeline(df, col_to_drop)
    clean_pipeline_model = clean_pipeline.fit(df)
    df_cleaned = clean_pipeline_model.transform(df)
    return df_cleaned