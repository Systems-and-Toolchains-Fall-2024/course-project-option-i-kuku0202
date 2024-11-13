[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/VuODydzp)


**Name**: Yue Su  
**AndrewID**: yuesu 

**Name**: Junyi Xu  
**AndrewID**: johnx


# Task 1
## Description of the column features:
- **sofifa_id**:People unique id in FIFA
- **player_url**:The url to the player website
- **short_name**: Commonly known name for player.
- **long_name**: Full name for player.
- **player_positions**: Position the player plays.
- **overall**: Overall rating of the player.
- **potential**: Potential rating of the player.
- **value_eur**: The player value in euro.
- **wage_eur**: Weekly wage of the player in euros.
- **age**: Age of the player.
- **dob**: Date of birth of the player.
- **height_cm**: Height of the player in cm.
- **weight_kg**: Weight of the player in kg.
- **club_team_id**: Unique identifier for club team.
- **club_name**: Name of the club team.
- **league_name**: Name of the league.
- **league_level**: Level or division of the league.
- **club_position**: Position the player holds at the club.
- **club_jersey_number**: Jersey number to the player at the club.
- **club_loaned_from**: Club from which the player is loaned.
- **club_joined**: Date when the player joined the club.
- **club_contract_valid_until**: Expiration year of the player's club contract.
- **nationality_id**: Unique identifier for the player's nationality.
- **nationality_name**: Nationality of the player.
- **nation_team_id**: Identifier for the national team.
- **nation_position**: Position the player holds in the national team.
- **nation_jersey_number**: Jersey number assigned to the player in the national team.
- **preferred_foot**: Preferred foot of the player.
- **weak_foot**: Rating of the player’s weak foot.
- **skill_moves**: Skill moves rating.
- **international_reputation**: International reputation rating.
- **work_rate**: Work rate of the player's.
- **body_type**: Body type classification of the player.
- **real_face**: Whether the player's face is a realistic likeness.
- **release_clause_eur**: Release clause value of the player in euros.
- **player_tags**: Tags describing player characteristics.
- **player_traits**: Traits indicating unique player abilities.
- **pace**: Overall pace rating of the player.
- **shooting**: Overall shooting ability rating.
- **passing**: Overall passing ability rating.
- **dribbling**: Overall dribbling ability rating.
- **defending**: Overall defending ability rating.
- **physic**: Overall physical strength rating.
- **attacking_crossing**: Ability to deliver crosses accurately.
- **attacking_finishing**: Ability to finish goal-scoring chances.
- **attacking_heading_accuracy**: Accuracy of heading the ball.
- **attacking_short_passing**: Ability to make short passes accurately.
- **attacking_volleys**: Ability to score from volleys.
- **skill_dribbling**: Dribbling skill rating.
- **skill_curve**: Ability to curve the ball, useful for shots and passes.
- **skill_fk_accuracy**: Accuracy in free kicks.
- **skill_long_passing**: Accuracy in long-distance passes.
- **skill_ball_control**: Ability to control the ball accurately.
- **movement_acceleration**: Acceleration speed.
- **movement_sprint_speed**: Top sprinting speed.
- **movement_agility**: Agility and ease of movement.
- **movement_reactions**: Reaction time and responsiveness.
- **movement_balance**: Physical balance.
- **power_shot_power**: Power in shooting the ball.
- **power_jumping**: Jumping ability.
- **power_stamina**: Stamina level for maintaining performance.
- **power_strength**: Physical strength.
- **power_long_shots**: Ability to take accurate long shots.
- **mentality_aggression**: Level of aggression.
- **mentality_interceptions**: Ability to intercept the ball.
- **mentality_positioning**: Positional awareness.
- **mentality_vision**: Ability to see and make opportunities.
- **mentality_penalties**: Ability to take penalty kicks.
- **mentality_composure**: Composure under pressure.
- **defending_marking_awareness**: Awareness in marking opponents.
- **defending_standing_tackle**: Ability in standing tackles.
- **defending_sliding_tackle**: Ability in sliding tackles.
- **goalkeeping_diving**: Diving ability for goalkeepers.
- **goalkeeping_handling**: Handling ability for goalkeepers.
- **goalkeeping_kicking**: Kicking ability for goalkeepers.
- **goalkeeping_positioning**: Positioning awareness for goalkeepers.
- **goalkeeping_reflexes**: Reflexes for goalkeepers.
- **goalkeeping_speed**: Speed for goalkeepers.
- **player_face_url**: URL linking to an image of the player’s face.
- **club_logo_url**: URL linking to the club’s logo image.
- **club_flag_url**: URL linking to the club’s country flag image.
- **nation_logo_url**: URL linking to the national team’s logo image.
- **nation_flag_url**: URL linking to the player’s nationality flag image.
- **year**: Year of the dataset entry.
- **gender**: Gender of player.
- **unique_id**: Unique identifier for the dataset entry.

The following fields represent positional ratings, indicating a player's ability to perform in each field position:
- **ls, st, rs, lw, lf, cf, rf, rw, lam, cam, ram, lm, lcm, cm, rcm, rm, lwb, ldm, cdm, rdm, rwb, lb, lcb, cb, rcb, rb, gk**: Ratings for the respective positions.


### Why PostgreSQL over NoSQL?
This dataset has a consistent, well-defined structure, which fits well in the table-based schema of PostgreSQL. The PostgreSQL ensures data integrity and supports ACID transactions. It is easy to execute complex SQL queries, combining tables, filter data, and perform aggregations. NoSQL databases are better suited for unstructured data or flexible schema scenarios that change frequently. So it does not fit this table well.

# Task 2
## Please our results on our code


# Task 3
## Data Clearning
We conduct the following operations to clean the data:
1. Remove redundant columns (columns unhelpful to inferring outcome)
2. Convert positional ratings to numeric value by adopting the first half
3. Remov columns with more than 1/5 of entires missing
4. Perform imputation on the rest of columns with NaN entiires based on the column data type:
    - Impute with mean value if the column is of `FloatType()`
    - Impute with median value if the column is of `IntegerType()`
    - Impute with mode value if the column is of `StringType()`

The steps are as follows:
- Step 1: Import everything from `preprocess.py
- Step 2: Load original data from pyAdmin
- Step 3: Run `clean_data(df)` function to get the cleaned dataframe

## Data Engineering
We perform data engineering by following these steps:\
- 1. Cast columns with continuous values into `FloatType()`
- 2. Rename the ground truth column `overall` to `outcome`
- 3. Apply string indexer and one-hot-encoder to columns with nominal and binary values
- 4. Vectorize all feature columns
- 5. Scale the vectorized feature column to prepare for the model training
- 6. Drop redundant columns

All procedures are grouped into a single pipline and can be directly applied to the clearned dataframe

## Pyspark implementation
We use Linear Regression and Gradient Boosted Decision Tree to train and test the data. Below are the details of training and testing for each model:
### Linear Regression
- 1. Split the data into training and testing set
- 2. Build the Linear Regression model and regression evaluator
- 3. Create the parameter grid for hyperparameter tuning. For the tuning parameter, we select `regParam` and `elasticNetParam`
    - `regParam`: Regularization to prevent model overfitting
    - `elasticNetParam`: Type of regularization to use (L1, L2, or mix)
- 4. Create cross validator using the model, parameter grid, and evaluator
- 5. Fit the model to the training data with 5-fold cross validation
- 6. Record the mode with best performance and evaludate it on the testing set.
### Gradient Boosted Decision Tree
- 1. Split the data into training and testing set
- 2. Build the Gradient Boosted Decision Tree model and regression evaluator
- 3. Create the parameter grid for hyperparameter tuning. For the tuning parameter, we select `maxDepth`, `maxIter`, and `stepSize`
    - `maxDepth`: Depth of the tree
    - `maxIter`: Maximum number of iteration
    - `stepSize`: Learning rate
- 4. Create cross validator using the model, parameter grid, and evaluator
- 5. Fit the model to the training data with 5-fold cross validation
- 6. Record the mode with best performance and evaludate it on the testing set.


## Pytorch implementation
- 1. We first split the data into training, validation, test in 0.6, 0.2, 0.2 and change the X and y into numpy in each dataset.
- 2. We have built two models, one is shallow(1 layer), one is deep(3 layers).
- 3. We use two learning rate: [0.01,0.001] and two batch size[32,64] to do the tuning. By doing the combinations, we get our best model: {'lr': 0.01, 'batch_size': 64} for simple, and {'lr': 0.01, 'batch_size': 32} for complex.
- 4. We then do the test and get our rmse: Simple: 2.5094, Multiple: 0.9050.