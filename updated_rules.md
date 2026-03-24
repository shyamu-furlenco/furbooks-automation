<!-- Opening revenue -->
1 If we are trying to find the opening revenue of march month. Then all those cases here is the two scenario
1.1) start_date in feb and recognised_at date is not in feb (Normal cycles)
1.2) start_date not in feb but recognised_at date in feb (mtp cases where cycles are in future)



<!-- MTP Cases -->
2) MTP current_month: recognised_date is current_month and start_date is in future. Now this mtp for current month will be MTP 1 for next month and that will be basically satisfied by logic 2 above.

<!-- Scenarios -->

Customer's cycle are as follows

Jan 15      Feb 14 100 Rs
Feb 15      Mar 14 100 Rs
Mar 15      Apr 14 100 Rs

1) If customer places return request on 20th of jan and we are computing components for january month, then under minimum tenure penalty(MTP) case, we get 200 Rs. according to the MTP case 2. Now this 200 Rs will be part of Feb month opening and this will be shown by 1.2 points under opening revenue.

2) If return as placed on 20th of feb, and we are calculating revenue for the month feb then under mtp for feb month we will have 100 Rs. For March opening revenue, we will have this component as discribed above. We also have to show this as separate entity(row in next month). We will subtract opening revenue(normal + mtp) - mtp as adjusted revenue.