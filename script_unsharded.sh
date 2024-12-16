#!/bin/bash

# MongoDB connection parameters
MONGO_HOST="MESIIN59202401030"
MONGO_PORT="30000"
DATABASE="rh"
ITERATIONS=10

# Helper function to calculate mean and stddev using awk
calc_stats() {
    # Expects numbers via stdin
    # Prints mean and stddev
    awk '{
        count+=1
        sum+=$1
        array[count]=$1
    }
    END{
        mean=sum/count
        sumsq=0
        for(i=1;i<=count;i++){sumsq+=(array[i]-mean)*(array[i]-mean)}
        if(count>1) {
            stddev=sqrt(sumsq/(count))
        } else {
            stddev=0
        }
        print mean, stddev
    }'
}

run_query() {
    local query_name=$1
    local js_query=$2

    echo "Executing $query_name..."

    times=()
    for i in $(seq 1 $ITERATIONS); do
        start=$(date +%s%N)
        # Run the query via mongo shell
        # --quiet to suppress extra output
        # --eval to evaluate the JS query
        mongo --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$DATABASE" --eval "$js_query" >/dev/null
        end=$(date +%s%N)
        elapsed_ns=$((end - start))
        elapsed_s=$(echo "$elapsed_ns" | awk '{printf "%.6f", $1/1000000000}')
        times+=("$elapsed_s")
        echo "Iteration $i: ${elapsed_s}s"
    done

    # Sort times and discard min and max if we have more than 2 measurements
    sorted_times=($(printf '%s\n' "${times[@]}" | sort -n))
    if [ ${#sorted_times[@]} -gt 2 ]; then
        trimmed_times=("${sorted_times[@]:1:${#sorted_times[@]}-2}")
    else
        trimmed_times=("${sorted_times[@]}")
    fi

    # Calculate mean and stddev
    stats=$(printf "%s\n" "${trimmed_times[@]}" | calc_stats)
    mean=$(echo "$stats" | awk '{print $1}')
    stddev=$(echo "$stats" | awk '{print $2}')

    echo "Average Execution Time (excluding max and min): ${mean}s"
    echo "Standard Deviation: ${stddev}s"
    echo
}

####################################
# Define Queries
####################################

# Note: We assume that all queries have been tested and work in MongoDB shell 3.6.8 as the user stated.
# We revert any Python date logic to original JS logic where needed.
# For month calculations, we use: (new Date().getMonth()+2)%12||12 as provided originally.
# For $date:"NOW", we use new Date().

# For find queries, we do: db.collection.find(filter, projection).toArray()
# For aggregate queries, we do: db.collection.aggregate(pipeline).toArray()

# Replace $date:"NOW" with new Date() in queries that use it.

# Queries denormalization 1 (d1_unsharded)
Q1='db.d1_unsharded.find({ firstName: "Meredith", lastName: "Coomes" }, { email: 1, phoneNumber: 1, "location.city": 1, "location.state": 1, _id: 0 });'
Q2='db.d1_unsharded.find({ "location.city": "New York City" }, { firstName: 1, lastName: 1, jobTitle: 1, "location.city": 1, _id: 0 });'
Q3='db.d1_unsharded.find({ "location.city": "New York City" }, { firstName: 1, lastName: 1, jobTitle: 1, "location.city": 1, _id: 0 });'
Q4='db.d1_unsharded.aggregate([ { $addFields: { convertedDate: "$dateOfJoining" } }, { $project: { firstName: 1, lastName: 1, dateOfJoining: 1, monthOfJoining: { $month: "$convertedDate" } } }, { $match: { monthOfJoining: ((new Date().getMonth() + 2) % 12) || 12 } } ]);'
Q5='db.d1_unsharded.aggregate([ { $group: { _id: "$location.city", averageSalary: { $avg: "$salary" } } }, { $sort: { averageSalary: -1 } } ]);'
Q6='db.d1_unsharded.aggregate([{ $addFields: { convertedDate: { $cond: { if: { $eq: [{ $type: "$dateOfJoining" }, "string"] }, then: { $dateFromString: { dateString: "$dateOfJoining" } }, else: "$dateOfJoining" } } } }, { $group: { _id: { yearOfJoining: { $year: "$convertedDate" } }, employeeCount: { $sum: 1 }, averageTenure: { $avg: { $divide: [ { $subtract: [new Date(), "$convertedDate"] }, 1000 * 60 * 60 * 24 * 365 ] } } } }, { $sort: { "_id.yearOfJoining": 1 } } ]);'
Q7='db.d1_unsharded.aggregate([{ $addFields: { convertedDate: { $cond: [{ $eq: [{ $type: "$dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$dateOfJoining" } }, "$dateOfJoining"] }, yearsOfService: { $divide: [{ $subtract: [new Date(), { $cond: [{ $eq: [{ $type: "$dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$dateOfJoining" } }, "$dateOfJoining"] }] }, 1000 * 60 * 60 * 24 * 365] }, lastPercentHikeNumeric: { $divide: [{ $subtract: [{ $strLenCP: "$lastPercentHike" }, 1] }, 1] } } }, { $project: { yearsOfService: 1, lastPercentHikeNumeric: 1 } }]);'
Q8='db.d1_unsharded.aggregate([{ $addFields: { convertedDate: { $cond: [{ $eq: [{ $type: "$dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$dateOfJoining" } }, "$dateOfJoining"] }, yearsOfService: { $divide: [{ $subtract: [new Date(), { $cond: [{ $eq: [{ $type: "$dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$dateOfJoining" } }, "$dateOfJoining"] }] }, 1000 * 60 * 60 * 24 * 365] } } }, { $group: { _id: "$location.region", totalEmployees: { $sum: 1 }, averageSalary: { $avg: "$salary" }, averageYearsOfService: { $avg: "$yearsOfService" } } }, { $sort: { totalEmployees: -1, averageSalary: -1, averageYearsOfService: -1 } }]);'

# Queries denormalization 2 (d2_unsharded)
Q9='db.d2_unsharded.find({ "employeesList.firstName": "Meredith", "employeesList.lastName": "Coomes" }, { "employeesList.email": 1, "employeesList.phoneNumber": 1, "city": 1, "state": 1, "placeName": 1 });'
Q10='db.d2_unsharded.find({ "city": "New York City" }, { "employeesList.firstName": 1, "employeesList.lastName": 1, "employeesList.jobTitle": 1 });'
Q11='db.d2_unsharded.find({ "city": "New York City" }, { "employeesList.firstName": 1, "employeesList.lastName": 1, "employeesList.jobTitle": 1 });'
Q12='db.d2_unsharded.aggregate([{ $unwind: "$employeesList" }, { $addFields: { monthOfJoining: { $month: { $cond: [{ $eq: [{ $type: "$employeesList.dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$employeesList.dateOfJoining" } }, "$employeesList.dateOfJoining"] } } } }, { $match: { monthOfJoining: ((new Date().getMonth() + 2) % 12) || 12 } }, { $project: { _id: 0, "employeesList.firstName": 1, "employeesList.lastName": 1, "employeesList.dateOfJoining": 1, monthOfJoining: 1 } }]);'
Q13='db.d2_unsharded.aggregate([{ $unwind: "$employeesList" }, { $group: { _id: "$placeName", averageSalary: { $avg: "$employeesList.salary" } } }, { $project: { _id: 1, averageSalary: 1 } }]);'
Q14='db.d2_unsharded.aggregate([{ $unwind: "$employeesList" }, { $addFields: { yearOfJoining: { $year: { $cond: [{ $eq: [{ $type: "$employeesList.dateOfJoining" }, "string"] }, { $dateFromString: { dateString: "$employeesList.dateOfJoining" } }, "$employeesList.dateOfJoining"] } } } }, { $group: { _id: "$yearOfJoining", totalEmployees: { $sum: 1 } } }, { $sort: { _id: 1 } }, { $group: { _id: null, yearlyData: { $push: { year: "$_id", totalEmployees: "$totalEmployees" } }, totalEmployeesOverall: { $sum: "$totalEmployees" } } }, { $unwind: "$yearlyData" }, { $project: { year: "$yearlyData.year", totalEmployees: "$yearlyData.totalEmployees", retentionRate: { $multiply: [{ $divide: ["$yearlyData.totalEmployees", "$totalEmployeesOverall"] }, 100] } } }, { $sort: { year: 1 } }]);'
Q15='db.d2_unsharded.aggregate([{ $unwind: "$employeesList" }, { $addFields: { lastPercentHikeNumeric: { $divide: [{ $subtract: [{ $strLenCP: "$employeesList.lastPercentHike" }, 1] }, 1] } } }, { $project: { _id: 0, employeeID: "$employeesList.empID", firstName: "$employeesList.firstName", lastName: "$employeesList.lastName", ageInCompanyYears: "$employeesList.ageInCompanyYears", lastPercentHike: "$lastPercentHikeNumeric" } }, { $sort: { ageInCompanyYears: 1 } }]);'
Q16='db.d2_unsharded.aggregate([{ $unwind: "$employeesList" }, { $group: { _id: "$region", totalEmployees: { $sum: 1 }, avgSalary: { $avg: "$employeesList.salary" }, avgYearsOfService: { $avg: "$employeesList.ageInCompanyYears" } } }, { $sort: { totalEmployees: -1 } }, { $project: { _id: 1, totalEmployees: 1, avgSalary: { $divide: [{ $trunc: { $multiply: ["$avgSalary", 100] } }, 100] }, avgYearsOfService: { $divide: [{ $trunc: { $multiply: ["$avgYearsOfService", 100] } }, 100] } } }]);'

####################################
# Run Queries
####################################

run_query "Query1_unsharded_d1" "$Q1"
run_query "Query2_unsharded_d1" "$Q2"
run_query "Query3_unsharded_d1" "$Q3"
run_query "Query4_unsharded_d1" "$Q4"
run_query "Query5_unsharded_d1" "$Q5"
run_query "Query6_unsharded_d1" "$Q6"
run_query "Query7_unsharded_d1" "$Q7"
run_query "Query8_unsharded_d1" "$Q8"

run_query "Query9_unsharded_d2" "$Q9"
run_query "Query10_unsharded_d2" "$Q10"
run_query "Query11_unsharded_d2" "$Q11"
run_query "Query12_unsharded_d2" "$Q12"
run_query "Query13_unsharded_d2" "$Q13"
run_query "Query14_unsharded_d2" "$Q14"
run_query "Query15_unsharded_d2" "$Q15"
run_query "Query16_unsharded_d2" "$Q16"
