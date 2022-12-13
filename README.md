# multi-objective-recsys
 Multi-objective recommender system based on real-world e-commerce sessions.

# Context

Shoppers may be overwhelmed by the variety of products available from large retailers online. This can lead to empty carts and missed sales for retailers. Recommender systems that use data science to predict which products customers want can improve the online shopping experience. [**In this competition**](https://www.kaggle.com/competitions/otto-recommender-system/overview), participants will build a model to **predict click-through**, **add-to-cart**, and **conversion rates**.


# Evaluation

Submissions are evaluated on Recall@20 for each action type, and the three recall values are weight-averaged:

$$score = 0.10 \cdot R_{\text{clicks}} + 0.30 \cdot R_{\text{carts}} + 0.60 \cdot R_{\text{orders}}$$

Where Recall@20 is defined as 

$$R_{\text{type}} = \frac{\text{True predictions (truncated to first 20)}}{\text{min(20, } \text{number of ground truth aids}\text{)}}$$

For each `session` in the test data, your task it to predict the `aid` values for each `type` that occur after the last timestamp `ts` the test session. In other words, the test data contains sessions truncated by timestamp, and you are to predict what occurs after the point of truncation.

For `clicks` **there is only a single ground truth value for each session**, which is the next aid clicked during the session (although you can still predict up to 20 `aid` values). The ground truth for `carts` and `orders` contains all `aid` values that were added to a cart and ordered respectively during the session.

![image](https://user-images.githubusercontent.com/109352381/207361386-01ab3300-4313-4353-94ca-94a494b47918.png)

Each `session` and `type` combination should appear on its own `session_type` row in the submission, and predictions should be space delimited.

# Dataset Description

* **train.jsonl (11 GB)** - the training data, which contains full session data
  * `session` - the unique session id
  * `events` - the time ordered sequence of events in the session
  * `aid` - the article id (product code) of the associated event
  * `ts` - the Unix timestamp of the event
  * `type` - the event type, i.e., whether a product was clicked, added to the user's cart, or ordered during the session

* **test.jsonl (402 MB)** - the test data, which contains truncated session data
  * task is to predict the next `aid` clicked after the session truncation, as well as the the remaining `aids` that are added to carts and orders; you may predict up to 20 values for each session type

* **sample_submission.csv (184 MB)** - a sample submission file in the correct format
