import tensorflow as tf
import tensorflow_text

#MODEL_POSITION = '/Users/enzoveltri/software/google-cloud-sdk/download/exported_totto/1648032169' ## TODO: parameter
MODEL_POSITION = '/Users/enzoveltri/software/google-cloud-sdk/download/exported_totto_large/1648208035' ## TODO: parameter

class T5SentenceGenerator:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if T5SentenceGenerator.__instance == None:
            T5SentenceGenerator()
        return T5SentenceGenerator.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if T5SentenceGenerator.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            T5SentenceGenerator.__instance = self
        self.model_path = MODEL_POSITION
        #self.imported = tf.saved_model.load(MODEL_POSITION, ["serve"])
        self.imported = tf.saved_model.load(MODEL_POSITION)
        DEFAULT_FUNCTION_KEY = "serving_default"
        self.inference_func = self.imported.signatures[DEFAULT_FUNCTION_KEY]
        self.predict_fn = lambda x: self.inference_func(tf.constant(x))['outputs']

    def predict(self, input):
        input = "totto table: " + input
        prediction = self.predict_fn([input]).numpy()[0].decode("utf-8")
        return prediction
