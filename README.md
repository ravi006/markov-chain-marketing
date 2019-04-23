Markov Chain Model Case Study
=============================

Any consumer of product and services has a natural rhythm to his or her purchase history. Regular customers tend to visit a business according
to some temporal patterns inherent in their buying history of products and services.

##Markov Chain
Let S = { S 1, S 2, S 3, ...} be a set of finite states. We want to collect the following probabilities:
P ( Sn | Sn –1, Sn –2, ..., S 1)
Markov’s first-order assumption is the following: P ( Sn | Sn –1, Sn –2, ..., S 1) P ( Sn | Sn –1)
The Markov second-order assumption is the following: P ( Sn|Sn –1 ,Sn –2 , ..., S 1) P ( Sn|Sn –1 ,Sn –2)
Now, we can express the joint probability using the Markov assumption:
P(S1,S2, . . .,Sn) = P(Si, Si1) Where i = (1, n)
Assume we have historical customer transaction data that includes transaction-id, customer-id, purchase-date, and amount. Therefore, each input
record will have the following format:

<customerID>,<transactionID>,<purchaseDate>,<amount>
V31E55G4FI,1381872898,2013-01-01,123
301UNH7I2F,1381872899,2013-01-01,148
PP2KVIR4LD,1381872900,2013-01-01,163
AC57MM3WNV,1381872901,2013-01-01,188
BN020INHUM,1381872902,2013-01-01,116
UP8R2SOR77,1381872903,2013-01-01,183
VD91210MGH,1381872904,2013-01-01,204
COI4OXHET1,1381872905,2013-01-01,78
76S34ZE89C,1381872906,2013-01-01,105
6K3SNF2EG1,1381872907,2013-01-01,214


Our aim is to create one model from this data so predict next state of the user. So that we can send promotional email to the user.

Unique State Calculation:
Time Difference:We will calculate time difference between two transaction. If
1. diff < 30 days we will assign S
2. 30 < diff < 60 we will assign M
3. 60 < diff we will assign L
Amount Difference:
We will calculate price difference between two transaction.
1. priorAmount < 0.9 * currentAmount we will assign L
2. priorAmount < 1.1 * amount we will assign E
3. Else we will assign G
Example
Let’s say user has two transaction 123 and 124 and the data is like described below
Customer Transaction Id Purchase Date Amount
1 123 2013-01-01 200
1 124 2013-01-02 300
1 125 2013-02-03 150
In the above case:
we will get two states one for (124,123) and one for (125,124)
1. State for (124, 123)
a. Date difference between 124 and 123 is 1. So we will assign S
b. Price Difference between 124 and 123 is less than 0.9 of current. So we will assign L
c. So we will assign “SL“ for this state
2. State for (125, 124)
a. Date difference between 124 and 123 is more than 30. So we will assign M
b. Price Difference between 124 and 123 is less than 0.9 of current. So we will assign G
c. So we will assign “MG“ for this state
So from above three transaction we will get two states [' SL ', ' MG ']
The final step is to measure frequency count for user going from one state to another state. For the above data we will get <SL, MG> 1


3. **Spark run job**
    ## Before running Spark Job
        -- Keep Input raw data in HDFS Path `/markov/customer/` and result will be saved in ``/markov/out/``
        -- See the YAML File. 
    ## To Run this Spark Job locally
    `spark-submit state_frequency/state_runner/customer_state.py`