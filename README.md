# **Social Media Sentiment Analysis - README**

## **Overview**
This project analyzes social media posts and user engagement using Spark SQL DataFrames. The primary tasks include:
1. Identifying trending hashtags.
2. Understanding engagement behavior across age groups.
3. Analyzing the relationship between sentiment and engagement.
4. Ranking the most influential verified users.

## **Approach and Code Explanation for Each Task**

### **1. Hashtag Trends**

**Objective**:  
Identify trending hashtags by analyzing their frequency of use across all posts.

**Approach**:  
- Split the `Hashtags` column (which contains comma-separated hashtags) into individual hashtags.
- Count the frequency of each hashtag across all posts.
- Sort the hashtags in descending order by frequency to identify the most popular ones.

**Logic**:  
1. **Data Loading**: Read the `posts.csv` file into a Spark DataFrame.
2. **Splitting Hashtags**: Use `split()` and `explode()` functions to break the comma-separated hashtags into individual rows.
3. **Counting and Sorting**: Count the frequency of each hashtag and sort them in descending order.

**Output**:  
| Hashtag     | Count |
|-------------|-------|
| #bug        | 27    |
| #mood       | 26    |
| #love       | 25    |
| #design     | 25    |
| #fail       | 23    |
| #social     | 21    |
| #tech       | 17    |
| #AI         | 16    |
| #UX         | 15    |
| #cleanUI    | 13    |

---

### **2. Engagement by Age Group**

**Objective**:  
Understand how users from different age groups engage with content, based on likes and retweets.

**Approach**:  
- Join the `posts.csv` and `users.csv` datasets on `UserID`.
- Group the data by `AgeGroup` and calculate the average likes and retweets for each age group.

**Logic**:  
1. **Data Loading**: Load both `posts.csv` and `users.csv` into DataFrames.
2. **Join**: Perform an inner join between the two DataFrames on the `UserID` field.
3. **Aggregation**: Group by `AgeGroup` and calculate the average values for `Likes` and `Retweets`.

**Output**:  
| AgeGroup | Avg_Likes | Avg_Retweets |
|----------|-----------|--------------|
| Senior   | 64.79     | 21.68        |
| Teen     | 81.57     | 22.26        |
| Adult    | 70.62     | 24.22        |

---

### **3. Sentiment vs Engagement**

**Objective**:  
Evaluate how sentiment (positive, neutral, negative) influences post engagement.

**Approach**:  
- Categorize the posts based on their `SentimentScore`: Positive (`>0.3`), Neutral (`-0.3 to 0.3`), and Negative (`< -0.3`).
- Calculate the average likes and retweets for each sentiment category.

**Logic**:  
1. **Data Loading**: Read the `posts.csv` file into a DataFrame.
2. **Categorization**: Use `when()` and `col()` functions to categorize posts into positive, neutral, or negative sentiment groups.
3. **Aggregation**: Group by sentiment and calculate the average `Likes` and `Retweets`.

**Output**:  
| Sentiment | Avg_Likes | Avg_Retweets |
|-----------|-----------|--------------|
| Neutral   | 73.21     | 25.25        |
| Positive  | 77.27     | 22.24        |
| Negative  | 66.58     | 21.39        |

---

### **4. Top Verified Users by Reach**

**Objective**:  
Identify the top 5 verified users based on the total reach (sum of likes and retweets).

**Approach**:  
- Filter the users who have verified accounts.
- For each verified user, calculate their total reach by summing their `Likes` and `Retweets`.
- Rank the users by total reach.

**Logic**:  
1. **Data Loading**: Load both `posts.csv` and `users.csv` into DataFrames.
2. **Filtering**: Filter out the non-verified users from the `users.csv` DataFrame.
3. **Aggregation**: Group by `UserID`, calculate the total reach (`Likes + Retweets`), and sort by reach in descending order.

**Output**:  
| Username        | TotalReach |
|-----------------|-----------|
| @pixel_pusher   | 2071      |
| @techie42       | 1948      |
| @stream_bot     | 1315      |
| @calm_mind      | 1251      |
| @daily_vibes    | 873       |

---

## **How to Run the Code**

### **1. Setup Instructions**

- Ensure Python 3.x, PySpark, and Apache Spark are installed.
- Clone the repository and place the CSV files (`posts.csv`, `users.csv`) in the `input/` directory.
- Use the following commands to run the scripts:

```bash
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py
