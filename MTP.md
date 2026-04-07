<!-- Scenario 1 -->

    Start_Date          End_date            Recognised_at
November 8, 2025	December 7, 2025	December 7, 2025, 7:00 PM
December 8, 2025	January 7, 2026	  	December 12, 2025, 5:33 PM
January 8, 2026	    February 7, 2026 	December 12, 2025, 5:33 PM

MTP current_month logic: recognised_date is current_month and start_date is in future i.e. Recognised in december and start_date > december 31, 2025

Problem : If the return is placed on 12 of december according to the logic if we are doing calculation of december month opening, 2nd cycle will be skipped.


<!-- Scenario 2 -->

    Start_Date          End_date            Recognised_at
November 8, 2025	December 7, 2025	    December 4, 2025, 7:00 PM
December 8, 2025	February 7, 2026	  	December 4, 2025, 7:00 PM

