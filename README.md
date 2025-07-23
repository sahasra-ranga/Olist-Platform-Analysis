# Analysing the business data of Olist, a Brazillian based e-commerce platform to evaluate how their performance can be enhanced

## Background and Overview
Olist is a Brazillian **e-commerce platform**, that connects small and medium-sized businesses with major marketplaces like Walmart and B2W. It acts as a bridge, enabling sellers to manage listings, inventory, orders, and shipments, while customers can browse and purchase products. Through this platform, costs are lowered for sellers and operations are streamlined due to the optimisation of supply chain management.

This project analyses and evaluates 3 years of data, to answer this key question, **"How can Olist improve its business performance?"**.

Insights are reccomendations are based on these categories:
1. Sales Trends 
2. Customer Repurchase Rate 
3. Yearly and Monthly Trends
4. Return Rate 
5. Product Categories and Merchant 
6. Customer Satisfaction 
7. Payment Type
8. Logistics 

## Data Structure and Overview
The data used consists of 8 datasets, with a total row count of approximately 1,100,000 records.

<img width="373" height="661" alt="Screenshot 2025-07-23 at 12 30 43 PM" src="https://github.com/user-attachments/assets/8ed98302-a4bf-46e5-a424-3491785e81bd" />
<img width="371" height="553" alt="Screenshot 2025-07-23 at 12 30 13 PM" src="https://github.com/user-attachments/assets/4261e510-c47d-431f-bb7c-e3073064ba79" />
<img width="380" height="489" alt="Screenshot 2025-07-23 at 12 29 19 PM" src="https://github.com/user-attachments/assets/8b408a9f-3709-4cdb-903f-68482906e41b" />
<img width="380" height="411" alt="Screenshot 2025-07-23 at 12 31 12 PM" src="https://github.com/user-attachments/assets/33ee7896-1e3b-45aa-9e79-97094fb46a02" />

Prior to analysing the data, a series of data cleaning, checks and standardisation were done using Python libraries, primarily NumPy and Pandas. The codes can be found in the check.py and data_cleaning.py files.

## Executive Summary

## Analysis
The following visualisations (apart from the word cloud) were done via Tableau 

**1.** **Measure Olist's business performance through customer satisfaction -- how have average review ratings changed with time?**
   <img width="556" height="438" alt="Screenshot 2025-07-23 at 2 00 01 PM" src="https://github.com/user-attachments/assets/1a3920eb-d1fe-43c5-9aa6-9968371ea24f" />
   Through this line chart, we can see that Olist faces the reality of stalled business quality growth in terms of stagnant platform review score

**2**. **How do freight costs vary across the different states in Brazil?**
   The freight costs were calculated via the following formula
   <img width="489" height="69" alt="Screenshot 2025-07-23 at 2 07 38 PM" src="https://github.com/user-attachments/assets/ac9f6f47-bb7c-4b97-ae6d-5cf23297bd30" />


   A choropleth graph is used to analyse these costs
   
   <img width="518" height="438" alt="Screenshot 2025-07-23 at 2 05 01 PM" src="https://github.com/user-attachments/assets/27f3ce69-d480-4857-8d22-2811d347a138" />
   Southern states of Brazil are more urbanized and hence have better built logistics and transport infrastructure.

   The distribution of sellers and customers is also analysed through a choropleth map
   <img width="928" height="451" alt="Screenshot 2025-07-23 at 2 11 49 PM" src="https://github.com/user-attachments/assets/2ad5cf46-2073-46ff-8f8a-d0b54778ea53" />

**3. Identifying customer pain points**
   Text analysis of customer reviews was conducted to dig deeper into their pain points. However, since the reviews were in Portuguese, translation had to be conducted after the 
   analysis. A word cloud as well as a treemap was used to analyse these reviews.
   
   <img width="373" height="400" alt="Screenshot 2025-07-23 at 2 15 17 PM" src="https://github.com/user-attachments/assets/a87fb19b-be34-4d4f-9563-238b7de4f280" />

   <img width="570" height="326" alt="Screenshot 2025-07-23 at 2 15 37 PM" src="https://github.com/user-attachments/assets/865bd567-85f8-40fc-b3b5-bd094691ac40" />

   Delivery delay is highlighted as the top most pertinent concern and dissatisfaction amongst customers as the negative words such as aguard (‘wait’ in English), are used most 
   frequently, amongst other secondary issues like product refunds.

   Let us evaluate further how if and how delivery delays impact customer review scores
   
   <img width="408" height="255" alt="Screenshot 2025-07-23 at 2 19 29 PM" src="https://github.com/user-attachments/assets/722fcf04-af06-496e-af00-1a27643087c6" />
   <img width="546" height="331" alt="Screenshot 2025-07-23 at 2 19 52 PM" src="https://github.com/user-attachments/assets/70e77860-957b-4436-a527-929cf63a7335" />

   We can see that these delays very obviously impact customer satisfaction, with median review scores decreasing from 5 to 2. However, to analyse this further, the tolerance for this 
   delay is looked into via the histogram, and we can estimate a tolerance of approximately 7 days before the review scores take a large dip.
   
   However, to narrow the issue down further, delays by region is analysed through another choropleth map.

   <img width="412" height="473" alt="Screenshot 2025-07-23 at 2 24 26 PM" src="https://github.com/user-attachments/assets/928dc727-e384-4b4d-8310-89dd62e448d5" />
   
   High Late Deliveries in the Southeast & South: 
   States like São Paulo, Minas Gerais, and Rio de Janeiro have the darkest shades, indicating the highest number of late deliveries. This aligns with the fact that these regions have 
   the largest population and highest volume of e-commerce activity.

   Logistical & Infrastructure Challenges: 
   The disparity in late deliveries reflect differences in infrastructure, warehouse distribution, and delivery efficiency across states.

   ## This brings us to our 1st strategy:
   
   ## Strategy 1: Strengthen Sao Paulo (SP) Logistics
   Rationale: Centralization of user base and logistics partners 
   Most Olist sellers and customers originate from SP.
   SP is the logistics hub of Brazil, thus more freight options and connectivity

   (a) Communication with existing logistics partners:
   Set KPI in agreement clause to limit delivery delay to within 7 days, with compensation if beyond 7 days
 
   (b) Formalise SP taskforce to resolve customer dissatisfaction
   Introduce live chat or chatbot features to address customer issues promptly
   Regularly monitor frequency of negative keywords (e.g. aguard) to track logistical improvement

   (c) Beyond existing logistics partners:
   Source for last-minute hyper logistics services to improve fulfil order with minimal delay
   Bypass the need for logistics: Collaborate with local post offices / Sao Paulo Metro to establish self-collection centres in SP


  **4.** **How do different product categories perform?**
     - Which product categories have the greatest order volume?
        <img width="899" height="385" alt="Screenshot 2025-07-23 at 2 34 53 PM" src="https://github.com/user-attachments/assets/8722a19e-92e7-4910-967f-f735f50c9eb8" />
        



                Through this bar graph, we can see a heavy home-focused consumer preference, with 4 of the top 6 categories being under this category.

     - Which product categories bring in the greatest revenue?
        <img width="706" height="306" alt="Screenshot 2025-07-23 at 2 38 29 PM" src="https://github.com/user-attachments/assets/16789677-a47e-4808-b384-3491cae505bf" />

        Classification based on the graph:
        Premium Category: categories showing higher revenue positions than order count positions (watches_gifts, auto, cool_stuff) 
        Volume-driven Category: bed_bath_table and sports_leisure derive their revenue strength primarily from high transaction counts rather than high unit values.

        




   
   
















