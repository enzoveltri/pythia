import tensorflow as tf
import tensorflow_text

MODEL_POSITION = '../../data/model' ## TODO: arameter

class T5Engine:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if T5Engine.__instance == None:
            T5Engine()
        return T5Engine.__instance

    def __init__(self):
        #print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
        #print(type(tf.config.list_physical_devices()))
        for device in tf.config.list_physical_devices():
            print(device)
        """ Virtually private constructor. """
        if T5Engine.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            T5Engine.__instance = self
        self.model_path = MODEL_POSITION
        #self.predict_fn = self.load_predict_fn_old(self.model_path)
        #self.predict_fn = self.load_predict_fn(self.model_path)
        self.imported = tf.saved_model.load(MODEL_POSITION, ["serve"])
        #tf.saved_model.save(self.imported, "./modello/model.h5")

    def get_predict(self, input):
        return self.imported.signatures['serving_default'](tf.constant(input))['outputs'].numpy()


    def load_predict_fn(self, model_path):
        imported = tf.saved_model.load(model_path, ["serve"])
        return lambda x: imported.signatures['serving_default'](tf.constant(x))['outputs'].numpy()

    def load_predict_fn_old(self, model_path):
        if tf.executing_eagerly():
            print("Loading SavedModel in eager mode.")
            imported = tf.saved_model.load(model_path, ["serve"])
            return lambda x: imported.signatures['serving_default'](tf.constant(x))['outputs'].numpy()
        else:
            print("Loading SavedModel in tf 1.x graph mode.")
            tf.compat.v1.reset_default_graph()
            sess = tf.compat.v1.Session()
            meta_graph_def = tf.compat.v1.saved_model.load(sess, ["serve"], model_path)
            signature_def = meta_graph_def.signature_def["serving_default"]
            return lambda x: sess.run(
                fetches=signature_def.outputs["outputs"].name,
                feed_dict={signature_def.inputs["inputs"].name: x}
            )

    def _toKeyModel(self, val1, val2):
        return (val1+"***"+val2).replace(' ', '.')

    def makePrediction(self, input):
        return self.get_predict([input])[0].decode('utf-8')