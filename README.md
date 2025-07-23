# Analysing the business data of Olist, a Brazilian based e-commerce platform to evaluate how their performance can be enhanced

## Background and Overview
Olist is a Brazilian **e-commerce platform** that connects small and medium-sized businesses with major marketplaces like Walmart and B2W. It acts as a bridge, enabling sellers to manage listings, inventory, orders, and shipments, while customers can browse and purchase products. Through this platform, costs are lowered for sellers and operations are streamlined due to the optimisation of supply chain management.

This project analyses and evaluates 3 years of data, to answer this key question, **"How can Olist improve its business performance?"**.

Insights are recommendations are based on these categories:
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
     Which product categories have the greatest order volume?
    <img width="899" height="385" alt="Screenshot 2025-07-23 at 2 34 53 PM" src="https://github.com/user-attachments/assets/8722a19e-92e7-4910-967f-f735f50c9eb8" />

   Through this bar graph, we can see that there is a very strong home-focused consumer preference, with 4 out of the top 6 categories falling under this category.

   <img width="706" height="306" alt="Screenshot 2025-07-23 at 2 38 29 PM" src="https://github.com/user-attachments/assets/710b02be-d64c-450f-a399-9297fd8e791c" />
   Classification based on the graph
   Premium Category: categories showing higher revenue positions than order count positions (watches_gifts, auto, cool_stuff) 

   Volume-driven Category: bed_bath_table and sports_leisure derive their revenue strength primarily from high transaction counts rather than high unit values.

   <img width="325" height="353" alt="Screenshot 2025-07-23 at 2 47 15 PM" src="https://github.com/user-attachments/assets/b265fd60-d947-4f71-9863-80e2614a6a38" />

   When analysing the review score across product categories, we can see that product categories such as CDs, DVDs, musicals and fashion clothing perform best in terms of highest 
   average review scores. To leverage upon this, Olist can perform increased platform exposure, or incentivise customers through bundle and loyalty discounts. On the other end of the 
   stick, product categories such as security and diapers & hygiene perform poorest. Olist can improve by performing more stringent seller vetting processes and audits for listers in 
   these categories. Additionally, further customer reviews can be conducted to identify the leading issues with these products.

 **5. Repurchase rate of various products and their respective product categories**
      Calculations used:
      <img width="908" height="409" alt="Screenshot 2025-07-23 at 2 53 51 PM" src="https://github.com/user-attachments/assets/83e62b3a-58d8-4cac-aaf8-c4d3da49e39b" />
      <img width="487" height="323" alt="Screenshot 2025-07-23 at 2 54 13 PM" src="https://github.com/user-attachments/assets/75011d46-81c6-4a8f-aeeb-b6e65173c12b" />

      Let us look further into which product categories garner the greatest repurchase rates
      
      <img width="948" height="302" alt="Screenshot 2025-07-23 at 2 56 54 PM" src="https://github.com/user-attachments/assets/2564064e-42b2-4fb8-b343-1b21c969d989" />
     
      Top performers (5-7 range): Bed/bath, furniture, sports, bags/accessories, home wares
      Mid-range performers (2-4 range): Home appliances, watches, garden tools, toys, perfumery
      Low performers (<2 range): Most electronics, clothing items, and specialized categories

## Strategy 2: Strategic Resource Allocation
Top Performers (Repurchase Score: 5-7):
 Categories like Bed & Bath, Furniture, Sports, Bags/Accessories, and Home Wares show strong customer loyalty and growth potential. Prioritize marketing and inventory resources to 
 enhance visibility and capitalize on repeat business.

Mid-Range Performers (Repurchase Score: 2-4):
 Categories such as Home Appliances, Watches, Garden Tools, Toys, and Perfumery exhibit moderate repurchase behaviour. Focus on targeted marketing and product optimization to improve 
 retention and increase purchase frequency.

Low Performers (Repurchase Score: <2):
 Categories like Electronics, Clothing, and Specialized Items show low repurchase rates. Assess the reasons behind this and consider refining offerings, exploring new acquisition 
 strategies, or reducing investment in underperforming segments.

  **6. Seasonal Trends**
     How do sales volumes change across the top 5 selling product categories?
     <img width="576" height="315" alt="Screenshot 2025-07-23 at 3 03 35 PM" src="https://github.com/user-attachments/assets/71b36c33-7a0f-4d9b-b8fc-ff385ada3747" />

     The graph shows the quarterly trend of 5 most popular products. From the graph, we can observe:
     Overall Declining Trend 
     Seasonal Spike in Q2
     Q4 Performance Drop
     
## Strategy 3: Dynamic Pricing Strategies
Peak Demand (Q2 - Seasonal Spike):
 1. Increase prices during the seasonal spike in Q2 to maximize margins while maintaining demand.
 2. Off-Peak (Q4 - Performance Drop):
    Offer strategic discounts in Q4 to maintain sales, using tiered or bundled promotions to boost volume and retain customers.
3.  Overall Decline:
    Gradually lower prices over time to stay competitive and manage declining demand.

  **7. Payment Method Distribution**

  <img width="475" height="360" alt="Screenshot 2025-07-23 at 3 08 29 PM" src="https://github.com/user-attachments/assets/ebd0ed00-d6af-46c7-8548-6e24a48a086c" />

  We can see that a large majority (73.9%) of customers pay by credit card, with Boleto being the runner-up at just 19%.

  Olist can capitalize on the psychology of credit card swiping via partnerships and provide longer deadlines for Boleto payments to increase sales.

  Additionally, Olist can ensure current payment interface and infrastructure includes new in-trend payment types like Pix. (proactive approach)

## Strategy 4: Olist Platform Optimisation
Rationale: The platform itself can influence customer purchase behaviour.

Strategic fine-tuning of platform features can increase or create customer wants via boosted attractiveness and relevance of products (e.g. exploit network effect)
Making purchases easy, simple and convenient can increase purchase frequencies.

(a) AI-driven recommendation systems on application’s home page and order checkout page to enhance product delivery
Use ML algorithms to suggest historical purchases and encourage repeat buying.
Highlight top-selling products through banners and awards to leverage social proof. 
Feature discounted off-season items to address product seasonality.
Create product bundles combining high-volume and high-value items to increase average order value.

(b) Seamless Payment & Pricing Strategies:
Partner with local credit card companies for credit card payment discounts (price strategy)
Include new payment options like Pix



   **8. Refund Rates**
    Refund rates are a huge loss of revenue for e-commerce platforms, because it indicates logistics and resources that are wasted.
    Let us analyse if there is a trend in refund rates across time, and if there is a reasoning behind these trends. 
    <img width="614" height="338" alt="Screenshot 2025-07-23 at 3 14 57 PM" src="https://github.com/user-attachments/assets/6795f486-44d5-42cd-84a1-e1aa43db62de" />
    
    Refund-related orders (i.e., those marked as canceled or unavailable) show spikes during the holiday seasons — especially November and December. There is year-round activity, but 
    the volume peaks toward the end of each year, likely tied to Black Friday, Christmas, and increased seasonal demand. Let us dig deeper to find a relationship between customer 
    reviews and these refunds
   <img width="608" height="309" alt="Screenshot 2025-07-23 at 3 16 53 PM" src="https://github.com/user-attachments/assets/bb7aee1b-7067-4d8f-86fd-c242e1085dd3" />

    The majority of reviews are positive, but spikes in negative reviews occur in specific months.
    
    Peaks observed in Feb, Nov, and Dec of 2017 & 2018.
    These align with Brazilian events/holidays: Carnival, Black Friday, Christmas.
    High order volumes during these periods likely strain logistics.
    Results in delivery delays, stockouts, and service issues.
    Indicates operational readiness lags behind demand surges.


## Strategy 5
1. Scalable Logistics: Partner with additional couriers and temporary warehouses during peak seasons.
2. Inventory Buffering: Stock high-demand products in advance to prevent stockouts.
3. Customer Communication: Set realistic delivery expectations and provide proactive delay updates.
4. Performance Monitoring: Implement real-time tracking of delivery KPIs during high-volume months.
5. Post-Peak Feedback Loop: Analyze reviews post-peak to identify gaps and refine future responses.

   **9. Expansion beyond Sao Paulo**

   <img width="415" height="433" alt="Screenshot 2025-07-23 at 3 21 45 PM" src="https://github.com/user-attachments/assets/73c1c6ef-6ca8-456c-b271-daf288910a64" />

   Coastal eastern cities such as Alagoas and Sergipe, belong to the 2nd hotspot location for customer origin, with shipping ports such as porte de saupe (5th largest). 

   Compared to SP (with Brazil’s busiest trade ports), the trade stress on industrial infrastructure on these cities are less (i.e. less congestion, less freight competition), 
   therefore potentially less delivery delay.

   Under-tapped business opportunity of bringing SP specialty beyond SP (potentially via international ship routes)

## Strategy 6: Expand beyond Sao Paulo
Rationale: Expand customer base and seller fleet can directly increase number of users of Olist platform.
Direct approach towards increasing sales volume and revenue

(a) Advertising of Olist platform in coastal Eastern cities [non-price strategy]
Via social media advertisement, road banners, etc.
Encourage local merchants to go online

(b) Establish local Olist service centres to ensure quality service delivery
Expansion comes with the need to maintain or better review scores (a measurement of service delivery standards)
Partnership with well-related local provincial carrier services to ensure swift delivery within the cities [vendor selection]
       



   

        

