import argparse
import numpy as np
import random

parser = argparse.ArgumentParser()
parser.add_argument("file", help = "the file in which the generated points will stored")
parser.add_argument("-n", "--number", default = "1000000", help = "number of points to generate", type = int)
args = parser.parse_args()

print()
print('\r%s |%s| %s%% %s' % ('Progress:', '-' * 50, "0.0", 'Complete'), end = '\r')

f = open(args.file,"w")

#number of generated points
num = args.number

#the three centers
c1 = (10,15)
c2 = (35,2)
c3 = (50,30)
centers = [c1,c2,c3]

counter = 0 #number of points generated
points = set()

while counter < num:
    center = centers[np.random.randint(0,3)]
    dist = np.random.gamma(1,5) #random distance following a gamma(1,5) distribution towards 0
    x = random.uniform(center[0] - dist, center[0] + dist) #abscissa of the random point
    sign = random.choice([-1,1])
    y = sign * np.sqrt( dist**2 - (x - center[0])**2 ) + center[1] #circle: (x-a)^2 + (y-b)^2 = r^2
    point = (x,y)
    l = len(points)
    points.add(point)
    if l != len(points):
        counter += 1
        if counter == 1 :
            f.write(str(point[0]) + "," + str(point[1]))
        else:
            f.write("\n" +str(point[0]) + "," + str(point[1]))

    #Progress Bar
    percent = ("{0:.1f}").format(100 * (counter / float(num)))
    filledLength = int(50 * counter // num)
    bar = '█' * filledLength + '-' * (50 - filledLength)
    print('\r%s |%s| %s%% %s' % ('Progress:', bar, percent, 'Complete'), end = '\r')
print()
