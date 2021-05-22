  
I = LOAD '$G' USING PigStorage(',') AS ( x:long, y:long );
G1 = GROUP I BY x;
G2 = FOREACH G1 GENERATE group, COUNT(I) AS count1;
G3 = GROUP G2 BY count1;
O = FOREACH G3 GENERATE group, COUNT(G2);
STORE O INTO '$O' USING PigStorage (',');