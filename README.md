# multi-objective-recsys
 Multi-objective recommender system based on real-world e-commerce sessions.

# Context

Shoppers may be overwhelmed by the variety of products available from large retailers online. This can lead to empty carts and missed sales for retailers. Recommender systems that use data science to predict which products customers want can improve the online shopping experience. [**In this competition**](https://www.kaggle.com/competitions/otto-recommender-system/overview), participants will build a model to **predict click-through**, **add-to-cart**, and **conversion rates**.

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