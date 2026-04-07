Here is one scenario for plan_transition. Suppose the first month is march month opening and second month is april month opening, with item count and user_count.
Now the, suppose the discount given in first month is 180.50 Rs. and in second month is 169.67 Rs.
Now, we want to find the gap between these two months for the same item count and user count. The gap can be calculated as if we compare the taxable amount of two months:
2-1 = 191.33 - 180.5 = 10.83 Rs.

2	Opening_revenue	1	1	191.3300018310547
1	Opening_revenue	1	1	180.5


2   Upfront_Discount	1	1	169.67
1   Upfront_Discount	1	1	180.50

The upfront discount gap can be calculated as:
1-2 = 180.5 - 169.67 = 10.83 Rs

Now, we can attribute this gap to the change in discount amount between the two months.
This means that the decrease in the upfront discount from 180.50 Rs. to 169.67 Rs. has resulted in an increase in the taxable amount by 10.83 Rs. for the same item count and user count.