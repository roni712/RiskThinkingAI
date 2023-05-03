from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import os

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel

# Create a new SparkSession
spark = SparkSession.builder.appName("FlaskApp").getOrCreate()

# Prediction Function
def predict_volume(input_data):
    print(os.curdir)

    model = RandomForestRegressionModel.load("savedModel/randomForestpredictionmodel/")

    # Assemble the features into a vector column
    assembler = VectorAssembler(inputCols=['vol_moving_avg', 'adj_close_rolling_med'], outputCol="features")
    input_data = assembler.transform(input_data)

    # Make predictions on the new data
    predictions = model.transform(input_data)

    # Select relevant columns and return the DataFrame
    output = predictions.select('prediction')
    return output

app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health():
    return jsonify({"message":'App is healthy!'})

@app.route('/predict', methods=['GET'])
def predict():
    # Get the values of the 'vol_moving_avg' and 'adj_close_rolling_med' query parameters
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))

    input_data = spark.createDataFrame([(vol_moving_avg, adj_close_rolling_med)], ['vol_moving_avg', 'adj_close_rolling_med'])

    predictions = predict_volume(input_data)
    # Convert the predictions to a JSON response
    output = {'predictions': predictions.select('prediction').collect()}
    return jsonify(output)


if __name__ == '__main__':
    app.run()

