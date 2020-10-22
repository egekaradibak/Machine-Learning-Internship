import numpy as np
import csv
import datetime
import operator
import multiprocessing

seedCnt = 6
filename = 'total_data_may20'
#filename = 'test'
iterationCnt = 50

# comment fields
ID     = 'unique_id'
HOTEL  = 'hotel_name'
RATE   = 'bubble_rating'
VECTOR = 'vector'
SEED   = 'seed'
SEED_DISTANCE = 'seed_distance'
SEED_DISTANCES = 'seed_distances'

# hotel fields
DISTANCES = 'distances'
FURTHEST = 'furthest'
COMMENTS = 'comments'
SEEDS = 'seeds'

#seed fields
MEMBER_COUNT = 'member_count'
HOTELS  = 'HOTELS'

rawdata = []
hotels = {}
seeds = {}

#lock = multiprocessing.Lock()
queue = multiprocessing.Queue()

vectorNames = []
commentResults = []
hotelResults = []
seedResults = []

def distance(a,b):
    dist = 0
    for i in range(len(a)):
        dist += (a[i] - b[i])**2
    dist = np.sqrt(dist)
    return dist

def add(a,b):
    for i in range(len(a)):
        a[i] += b[i]

def div(a,b):
    if (b == 0):
        return
    for i in range(len(a)):
        a[i] /= b

# Allocate to a seed
def iterate(arg):
    hkey, hotel, seeds = arg
    changed = 0;
    comment_seeds = {}
    try:
        for ckey in hotel[COMMENTS] :
            comment = hotel[COMMENTS][ckey]
            result = []
            seedDist = []
            newSeed = -1
            newDist = 999999
            for i in range(seedCnt):
                dist = distance(seeds[i][VECTOR], comment[VECTOR])
                seedDist.append(dist)
                if (dist < newDist):
                    newSeed = i
                    newDist = dist
            result.append(newSeed)
            result.append(newDist)
            result.append(seedDist)
            comment_seeds[ckey] = result
            #print(comment[ID] + " seed = " + str(comment[SEED]) + " distance = " + str(comment[SEED_DISTANCE]))
            #print(comment[SEED_DISTANCES])
    finally:
        #print(hkey+' process completed')
        queue.put(([hkey, comment_seeds]))
    
def iterateHotels():
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())

    iterateList = []
    for hkey in hotels:
        hotel = hotels[hkey]
        iterateList.append([hkey, hotel, seeds])

    changed = 0
    pool.map(iterate, iterateList)
    pool.close()
    for i in range(len(iterateList)):
    #while not queue.empty():
        iterateResult = queue.get();
        hkey = iterateResult[0]
        comment_seeds = iterateResult[1]
        hotel = hotels[hkey]
        for ckey in comment_seeds:
            comment = hotel[COMMENTS][ckey]
            result = comment_seeds[ckey]
            newSeed = result[0]
            newDist = result[1]
            seedDist = result[2]
            if (comment[SEED] != newSeed):
                changed += 1
                comment[SEED] = newSeed
            comment[SEED_DISTANCE] = newDist
            comment[SEED_DISTANCES] = seedDist
    print('Chaned = ' + str(changed))

    return changed

def adjustSeeds():
    for i in range(seedCnt):
        seeds[i][VECTOR] = np.zeros([437])
        seeds[i][MEMBER_COUNT] = 0

    for hkey in hotels:
        hotel = hotels[hkey]
        for ckey in hotel[COMMENTS]:
            comment = hotel[COMMENTS][ckey]
            add(seeds[comment[SEED]][VECTOR], comment[VECTOR])
            seeds[comment[SEED]][MEMBER_COUNT] += 1

    for i in range(seedCnt):
        div(seeds[i][VECTOR], seeds[i][MEMBER_COUNT])

def buildCommentResults():
    global commentResults
    commentResults = []
    result = []
    result.append(ID)
    result.append(HOTEL)
    result.append(RATE)
    result.append(SEED)
    result.append(SEED_DISTANCE)
    result.append(SEED_DISTANCES)
    commentResults.append(result)
    for hkey in hotels :
        hotel = hotels[hkey]
        for ckey in hotel[COMMENTS] :
            comment = hotel[COMMENTS][ckey]
            result = []
            result.append(comment[ID])
            result.append(comment[HOTEL])
            result.append(comment[RATE])
            result.append(comment[SEED])
            result.append(comment[SEED_DISTANCE])
            result.extend(comment[SEED_DISTANCES])
            #print(result)
            commentResults.append(result)

def buildHotelResults():
    global hotelResults
    hotelResults = []
    result = []
    result.append(HOTEL)
    result.append(SEEDS)
    hotelResults.append(result)
    for hkey in hotels :
        hotel = hotels[hkey]
        result = []
        result.append(hkey)
        for skey in range(seedCnt):
            try:
                result.append(hotel[SEEDS][skey])
            except Exception as e:
                result.append(0)
        #print(result)
        hotelResults.append(result)

def buildSeedResults():
    global seedResults
    seedResults = []    
    for skey in range(seedCnt):
        seed = seeds[skey]
        result = []
        result.append(skey)
        result.append(seed[MEMBER_COUNT])
        vectorDictionary = {}
        for i in range(len(seed[VECTOR])):
            vectorDictionary[vectorNames[i]] = seed[VECTOR][i]
        sortedVector = {k: v for k, v in sorted(vectorDictionary.items(), key=lambda item: item[1], reverse=True)}
        #print(sortedVector)
        result.append(sortedVector)
        #print(result)
        seedResults.append(result)



def writeCommentResults():
    with open('./data/'+filename+'-cluster_'+str(seedCnt)+'-comments.csv', 'w') as f:
        writer = csv.writer(f)
        for result in commentResults:
            #print(result)
            writer.writerow(result)

def writeHotelResults():
    with open('./data/'+filename+'-cluster_'+str(seedCnt)+'-hotels.csv', 'w') as f:
        writer = csv.writer(f)
        for result in hotelResults:
            #print(result)
            writer.writerow(result)

def writeSeedResults(): 
    for result in seedResults:
        skey = result[0]
        with open('./data/'+filename+'-cluster_'+str(seedCnt)+'_'+str(skey)+'-seeds.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['seed', skey])
            writer.writerow([MEMBER_COUNT, result[1]])
            sortedVector = result[2]
            for ikey in sortedVector:
                item = sortedVector[ikey]
                #print(item[0] + str(item[1]))
                writer.writerow([ikey,item])

def main():
    global vectorNames
    global seeds
    global rawdata

    with open('./data/'+filename+'.csv', encoding="latin-1") as f:
        reader = csv.reader(f, delimiter=';')
        rawdata = list(reader)


    # Read csv file and create the hotels dictionary
    origin = np.zeros([437])
    commentCount = 0

    firstline = True
    for rawline in rawdata:
        if (firstline == True):
            vectorNames = rawline[11:]
            firstline = False;
            continue
        comment = {}
        comment[ID] = rawline[0]
        comment[HOTEL] = rawline[2]
        comment[RATE] = rawline[6]
        comment[SEED] = -1
        comment[SEED_DISTANCE] = 999999
        vector = []
        for i in range(len(rawline[11:])):
            v = int(rawline[11 + i])
            if (v > 2):
                v = 2
            vector.append(v)
        comment[VECTOR] = vector
        add(origin, vector)
        commentCount += 1
        try:
            hotels[comment[HOTEL]][COMMENTS][comment[ID]] = comment
        except Exception as e:
            hotel = {}
            hotel[COMMENTS] = {}
            hotel[COMMENTS][comment[ID]] = comment
            hotels[comment[HOTEL]] = hotel
    div(origin, commentCount)

    print(len(hotels))

    # identify the furthest commenst from the origin for each hotel and globally
    maxDist = 0
    maxKey  = ""
    maxHotel = ""
    for hkey in hotels:
        hotel = hotels[hkey]
        hotelMaxDist = 0
        hotelMaxKey  = ""
        for ckey in hotel[COMMENTS]:
            comment = hotel[COMMENTS][ckey]
            dist = distance(origin, comment[VECTOR])
            if (dist > hotelMaxDist):
                hotelMaxDist = dist
                hotelMaxKey = ckey
                if (dist > maxDist):
                    maxDist = dist
                    maxKey = ckey
                    maxHotel = hkey
        print( hkey + " " + hotelMaxKey + " " + str(hotelMaxDist))
        hotel[FURTHEST] = hotelMaxKey
        hotel[DISTANCES] = {}

    print(maxHotel  + " " + maxKey + " " + str(maxDist))

    # Calculate the distance between furthest hotel comments
    for hkey in hotels:
        hotel = hotels[hkey]
        for pkey in hotels:
            if hkey == pkey:
                continue
            photel = hotels[pkey]
            try:
                dist = photel[DISTANCES][hkey]
                hotel[DISTANCES][pkey] = dist
            except Exception as e:
                dist = distance(hotel[COMMENTS][hotel[FURTHEST]][VECTOR], photel[COMMENTS][photel[FURTHEST]][VECTOR])
                hotel[DISTANCES][pkey] = dist
                photel[DISTANCES][hkey] = dist
        

    # Identify the seeds
    seed = {}
    seed[ID] = maxKey
    seed[HOTEL] = maxHotel
    seed[VECTOR] = hotels[maxHotel][COMMENTS][maxKey][VECTOR]
    seed[HOTELS] = {}
    seeds[0] = seed
    for index in range(seedCnt):
        if index == 0:
            continue
        seed = {}
        maxDist = 0
        for hkey in hotels:
            skip = False
            for i in range(index):
                if hkey == seeds[i][HOTEL]:
                    skip = True
                    continue
            if skip:
                continue
            #print("seed search for " + str(index) + " - hkey = " + hkey)
            dist = 0
            hotel = hotels[hkey]
            for i in range(index):
                dist += hotel[DISTANCES][seeds[i][HOTEL]]
            if (dist > maxDist):
                maxDist = dist
                seed[ID] = hotel[FURTHEST]
                seed[HOTEL] = hkey
                seed[VECTOR] = hotels[hkey][COMMENTS][hotel[FURTHEST]][VECTOR]
                seed[HOTELS] = {}
                #print(str(maxDist) + " - " + str(seed))
        seeds[index] = seed
        print(str(index) + ' - ' + str(maxDist))
        #print(seeds[index])

    # Adjust seed centers closer to the center for better distribution
    for i in range(seedCnt):
        div(seeds[i][VECTOR], seedCnt)

    for i in range(iterationCnt):
        print(datetime.datetime.now())
        numchange = iterateHotels()
        print(datetime.datetime.now())
        print('adjusting Seeds ...')
        adjustSeeds()
        
        #print(seeds)
        #for hkey in hotels:
        #    hotel = hotels[hkey]
        #    print(hkey + ' - ' + str(hotel[SEEDS]))

        total = 0
        for j in range(seedCnt):
            print('seed ' + str(j) + ' member count = ' + str(seeds[j][MEMBER_COUNT]))
            total += seeds[j][MEMBER_COUNT]

        print('Iteration : ' + str(i) + ' - Changed = ' + str(numchange) + ' Total Recrods = ' + str(total))
        if (numchange == 0):
            break


    buildCommentResults()
    buildHotelResults()
    buildSeedResults()

    writeCommentResults()
    writeHotelResults()
    writeSeedResults()

if __name__ == "__main__":
    main()

"""

furthestIndex = 0
for j in range(len(dataSet)):
    if distance(origin,dataSet[j]) >= maxDist:
        furthestIndex = j
        maxDist = distance(origin,dataSet[j])

print(furthestIndex)

"The point furthestto the origin is dataPoint #33863"
"We can choose this as the first of our  samples"
"To find the next center, we can choose the point furthest to the coordinates of datapoint #33863"

#    ***How many centers do we need?***
#    ***How do we want to choose the centers?***

#ayni center icin tekrar hesaplama


numberOfCenters = 4
numberOfCenters = numberOfCenters - 1
distanceToCentersList = np.zeros([len(dataSet),numberOfCenters])
centerIndexList = []
distanceToCenterList = np.zeros([len(dataSet)])

centerIndexList.append (furthestIndex)
for i in range(numberOfCenters):
    maxdist = 0
    maxIndex = 0
    for j in range(len(dataSet)):
        dist = distance(dataSet[j], dataSet[centerIndexList[i]])
        #distanceToCentersList[j][i] = dist
        distanceToCenterList[j] += dist
        
        if maxdist <  dist:
            if j not in centerIndexList:
                maxdist = dist
                maxIndex = j
  
    centerIndexList.append(maxIndex)

print(centerIndexList)

"Find membership"
membershipList = []
'''
for i in range(numberOfCenters):
    membershipList.append([])

'''
for i in range(len(dataSet)):
    DIST = 999999999999999999
    index = 0
    print('ELEMENT', i)
    for j in range(len(centerIndexList)):
        dist = distance(dataSet[i],dataSet[centerIndexList[j]])
        
        print(dist)
        print(DIST)
        if dist <= DIST:
            DIST = dist
            index = j
    
        print(index)
    membershipList.append(index)
    
print(membershipList[15337:15340])

"Update Center"
centers = np.zeros([len(centerIndexList), 437])
howManyMembers = np.zeros([len(centerIndexList)])

for j in range(len(centerIndexList)):
    for i in range(len(membershipList)):
        if membershipList[i] == j:
            howManyMembers[j] += 1

for c in range(len(centerIndexList)):
    for i in range(len(membershipList)):
        if membershipList[i] == c:
            for feature in range(437):
                centers[c][feature] += dataSet[i][feature]/howManyMembers[c]
print(howManyMembers)
print(centers)

numberOfCenters = 4
def updateCenters():
    global centers
    global howManyMembers 
    centers = np.zeros([len(centerIndexList), 437])
    howManyMembers = np.zeros([len(centerIndexList)])
    
    for j in range(numberOfCenters):
        for i in range(len(membershipList)):
            if membershipList[i] == j:
                howManyMembers[j] += 1

    for c in range(numberOfCenters):
        for i in range(len(membershipList)):
            if membershipList[i] == c:
                for feature in range(437):
                    centers[c][feature] += dataSet[i][feature]/howManyMembers[c]

def updateMembership():
    global membershipList
    membershipList = np.zeros([len(dataSet)])
    for i in range(len(dataSet)):
        dist = 9999999
        index = 0
        for j in range(len(centers)):
            if distance(dataSet[i],centers[j]) <= dist:
                dist = distance(dataSet[i],dataSet[centerIndexList[j]])
                index = j
        membershipList[i] = index

i = 0
prevmembershipList = []
while (prevmembershipList!= membershipList):
    prevmembershipList = membershipList
    updateMembership()
    updateCenters()
    print(i)
    i = i +1
centers = np.zeros([len(centerIndexList), 437])
howManyMembers = np.zeros([len(centerIndexList)])

for j in range(len(centerIndexList)):
    for i in range(len(membershipList)):
        if membershipList[i] == j:
            howManyMembers[j] += 1
print(howManyMembers)

Adam & Eve - {3: 129, 1: 168, 4: 60, 0: 6, 2: 36}
Adora Resort Hotel - {3: 29, 2: 10, 1: 33, 4: 14, 0: 3}
Asteria Club Belek - {4: 71, 1: 168, 2: 66, 3: 119, 0: 12}
Belconti Resort Hotel - {2: 10, 1: 50, 3: 41, 4: 17, 0: 2}
Belek Soho Beach Club - {3: 8, 2: 2, 1: 8, 4: 2}
Bellis Deluxe Hotel - {3: 89, 1: 82, 4: 34, 2: 25, 0: 1}
Calista Luxury Resort - {1: 365, 4: 147, 3: 212, 2: 127, 0: 43}
Cesars Temple De Luxe - {1: 18, 3: 14, 2: 6, 4: 5, 0: 1}
Club Mega Saray - {1: 46, 3: 67, 4: 23, 2: 13, 0: 3}
Cornelia De Luxe Resort - {2: 301, 3: 328, 4: 262, 1: 378, 0: 65}
Cornelia Diamond Golf Resort & Spa - {2: 317, 4: 406, 1: 707, 3: 532, 0: 92}
Crystal Tat Beach Golf Resort & Spa - {3: 218, 1: 181, 4: 83, 2: 60, 0: 4}
Ela Quality Resort Belek - {1: 425, 3: 370, 2: 114, 4: 182, 0: 39}
FUN&SUN Club Belek - {1: 41, 2: 14, 4: 18, 3: 33, 0: 1}
Gloria Golf Resort - {4: 270, 2: 163, 1: 625, 3: 355, 0: 33}
Gloria Serenity Resort - {4: 225, 1: 641, 3: 372, 2: 169, 0: 49}
Gloria Verde Resort - {1: 403, 3: 263, 4: 183, 2: 88, 0: 29}
Granada Luxury Belek - {3: 226, 2: 29, 1: 234, 4: 60, 0: 6}
Green Max - {1: 13, 3: 8, 2: 3}
Gural Premier Belek - {1: 158, 3: 94, 4: 76, 2: 47, 0: 15}
IC Hotels Santai - {1: 196, 4: 104, 2: 81, 3: 152, 0: 19}
Innvista Hotels Belek - {1: 30, 3: 44, 4: 14, 2: 3, 0: 2}
Kaya Belek - {1: 412, 4: 251, 3: 452, 2: 194, 0: 47}
Kaya Palazzo Golf Resort - {3: 319, 4: 225, 1: 532, 0: 81, 2: 206}
Kempinski Hotel The Dome Belek - {4: 63, 1: 159, 3: 90, 0: 14, 2: 30}
Letoonia Golf Resort - {4: 299, 3: 377, 2: 253, 1: 537, 0: 48}
Limak Arcadia Sport Resort - {4: 394, 3: 404, 2: 228, 1: 424, 0: 65}
Limak Atlantis Deluxe Resort & Hotel - {4: 478, 1: 583, 3: 487, 2: 335, 0: 80}
Maritim Pine Beach Resort - {1: 135, 4: 65, 3: 109, 2: 81, 0: 21}
Maxx Royal Belek Golf Resort - {3: 373, 1: 1034, 4: 347, 2: 197, 0: 85}
Paloma Grida Resort & Spa - {1: 396, 3: 355, 4: 402, 0: 87, 2: 275}
Papillon Ayscha Hotel Resort & Spa - {3: 289, 4: 194, 1: 350, 2: 103, 0: 30}
Papillon Zeugma Relaxury - {1: 386, 3: 317, 0: 49, 2: 191, 4: 250}
Port Nature Luxury Resort Hotel & Spa - {1: 80, 4: 37, 3: 58, 2: 16, 0: 3}
Regnum Carya - {3: 359, 1: 841, 4: 339, 2: 185, 0: 77}
Rixos Premium Belek - {1: 918, 2: 206, 3: 573, 4: 319, 0: 64}
Robinson Club Nobilis - {3: 14, 1: 26, 4: 9, 2: 2, 0: 2}
Selectum Luxury Resort - {4: 72, 1: 115, 3: 103, 2: 22, 0: 7}
Sensitive Premium Resort & Spa - {3: 36, 1: 41, 2: 20, 0: 3, 4: 13}
Sirene Belek Hotel - {1: 242, 3: 207, 2: 87, 4: 125, 0: 11}
Spice Hotel & Spa - {3: 153, 4: 46, 1: 179, 0: 8, 2: 36}
Sueno Hotels Deluxe Belek - {3: 401, 1: 553, 4: 231, 2: 122, 0: 53}
Sueno Hotels Golf Belek - {3: 436, 1: 539, 4: 249, 2: 44, 0: 7}
Susesi Luxury Resort - {2: 224, 3: 331, 1: 508, 4: 211, 0: 83}
The Land of Legends Kingdom Hotel - {1: 716, 3: 371, 2: 33, 4: 159, 0: 12}
Titanic Deluxe Golf Belek - {1: 538, 3: 353, 4: 235, 2: 198, 0: 59}
TUI MAGIC LIFE Masmavi - {3: 107, 1: 153, 4: 76, 2: 96, 0: 19}
TUI BLUE Belek - Adults only - {3: 32, 1: 51, 2: 28, 4: 23, 0: 6}
Voyage Belek Golf & Spa - {2: 708, 1: 1347, 3: 770, 4: 907, 0: 234}
Xanadu Resort Hotel - {1: 416, 2: 199, 4: 249, 3: 276, 0: 64}
Zeynep Hotel - {3: 405, 1: 514, 4: 339, 2: 193, 0: 48}
seed 0 member count = 1792
seed 1 member count = 17695
seed 2 member count = 6196
seed 3 member count = 12260
seed 4 member count = 8863
Iteration : 18 - Changed = 121838 Total Recrds = 46806

"""