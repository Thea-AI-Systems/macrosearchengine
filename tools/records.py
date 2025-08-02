import re
import copy
import json

def label_to_topic(label, **kwargs):
    join_with = kwargs.get('join_with', ' ')
    retain_capitalization = kwargs.get('retain_capitalization', False)
    custom_conversions = kwargs.get('custom_conversions', [[r'\+', 'Plus ']])
    
    label = re.sub(r'\s+', ' ', label)

    #convert + to Plus - these are custom conversions will increase as we see more data    
    for conversion in custom_conversions:                
        label = re.sub(conversion[0], conversion[1], label)          
    
    #remove alpha numeric characters that are not spaces and &
    label = re.sub(r'[^a-zA-Z0-9\s&_]', '', label)       

    words = [w.strip() for w in label.split() if len(w.strip())>0]    
    
    if retain_capitalization:#retain underlying capitalization - capitalize only first letter
        words = [word[0].upper() + word[1:] for word in words]
        label = join_with.join(words)
    else:
        label = join_with.join([word.capitalize() for word in words])
    return label

def build_alltopics_field(topics_list):
    topics_str = "][".join(topics_list)
    topics_str = "[" + topics_str + "]"
    return topics_str

def number_convertor(num, **kwargs):    
    if num is None:
        return None
    if isinstance(num, str):
        num = num.replace(",", "")
        
        if kwargs.get('bracket_to_minus', False):
            num = num.replace("(", "-").replace(")", "")
    try:
        return float(num)
    except ValueError:
        return None

'''
    Define a Record class to hold the data
    and provide methods to manipulate the data
    Most important methods are how topics are created
    How extra fields are created
    How dimensions are created
'''
class DatabankRecord:
    def __init__(self, **kwargs):
        self.rec = {}            
        self.rec['dataset'] = kwargs.get('dataset', None)
        self.rec['ticker'] = kwargs.get('ticker', None) 
        self.rec['metric'] = kwargs.get('metric', None)
        self.rec['country'] = kwargs.get('country', None)
        self.rec['value'] = kwargs.get('value', None)        
        self.rec['value_txt'] = kwargs.get('value_txt', None)
        self.rec['period_end'] = kwargs.get('period_end', None)
        self.rec['period_span'] = kwargs.get('period_span', None)
        self.rec['unit'] = kwargs.get('unit', None)
        self.rec['created_by'] = kwargs.get('created_by', "MacroSearchEngine")
        self.rec['inter_country_comparison'] = kwargs.get('inter_country_comparison', False)
       
        if kwargs.get('dimensions') is not None:
            self.rec['dimensions'] = kwargs.get('dimensions')

        if kwargs.get('categories') is not None:
            self.rec['categories'] = kwargs.get('categories')
        
        if kwargs.get('all_topics') is not None:
            self.rec['all_topics'] = kwargs.get('all_topics')        

        if kwargs.get('updated_on') is not None:
            self.rec['updated_on'] = kwargs.get('updated_on')
        
        if kwargs.get('period_span') is not None:
            self.rec['period_span'] = kwargs.get('period_span')

        if kwargs.get('source') is not None:
            self.rec['source'] = kwargs.get('source')        
        
        self.search_fields = []

    def update_categories(self, label, category):
        if self.rec.get('categories') is None:
            self.rec['categories'] = {}
        if label not in self.rec['categories']:
            self.rec['categories'][label] = category

    def add_country(self, country):
        '''
            Country is a special field that is used to filter records
            It is not a topic, but a field in the record
        '''
        self.rec['country'] = country
        
    '''        
        Dimensions facilitate flexible yet precise search
        Every Dimension will have a TopicMaster Record
    '''
    def add_dimension(self, label, category):
        #check if it already exists
        if self.rec.get('dimensions') is None:
            self.rec['dimensions'] = []
        
        if label not in self.rec['dimensions']:
            self.rec['dimensions'].append(label)

        self.update_categories(label, category)

    '''
        Search Fields are to speed up searches in certain cases
        They will only be a part of categories and all_topics fields
    '''
    def add_search_field(self, label, category):        
        if label not in self.search_fields:
            self.search_fields.append(label)

        self.update_categories(label, category)

    '''
        value_txt = {
            "<key>": [
                {"label": "<label>", "weight": <weight>}
            ],
            ...
        }
    '''
    def add_constituent(self, label, weight, constituent_key):
        #find if label already exists
        if self.rec.get('value_txt') is None:
            self.rec['value_txt'] = {}
        if constituent_key not in self.rec['value_txt']:
            self.rec['value_txt'][constituent_key] = []        
        
        #add the new item        
        for rec in self.rec['value_txt'][constituent_key]:
            if rec['label'] == label:
                rec['weight'] = weight
                break
        else:
            self.rec['value_txt'][constituent_key].append({'label': label, 'weight': weight})            

    def prep_for_insert(self):        
        try:            
            #ticker + metric + dimensions + search_fields
            all_topics = [self.rec['ticker'], self.rec['metric'], self.rec['dataset']] + self.rec.get('dimensions', []) + self.search_fields            
            if self.rec.get('country') is not None:
                all_topics.append(self.rec['country'])
            all_topics = list(set(all_topics)) #remove duplicates
            all_topics_str = build_alltopics_field(all_topics)
            self.rec['all_topics'] = all_topics_str

            if self.rec.get('dimensions') is not None:                
                dimensions = list(dict.fromkeys(self.rec['dimensions']))  # remove duplicates, preserve order                                                
                dimensions_str = build_alltopics_field(dimensions)
                self.rec['dimensions'] = dimensions_str

            #update categories to a json
            
            if self.rec.get('categories') is not None:
                self.rec['categories'] = json.dumps(self.rec['categories'], ensure_ascii=False)            
            
        except Exception as e:
            print("Error in prep for insert field", e)
            raise e
    
    def update(self, **kwargs):
        for key, value in kwargs.items():            
            self.rec[key] = value
        
    def clone(self):
        newdbrec = DatabankRecord()
        newdbrec.rec = copy.deepcopy(self.rec)
        newdbrec.search_fields = copy.deepcopy(self.search_fields)
        return newdbrec        
    
    def validate_rec(self):
        if self.rec.get('ticker') is None:
            raise ValueError("Ticker is None")
        if self.rec.get('metric') is None:
            raise ValueError("Metric is None")
        if self.rec.get('period_end') is None:
            raise ValueError("Period end is None")
        if self.rec.get('value') is None:
            raise ValueError("Value is None")
        if self.rec.get('unit') is None:
            raise ValueError("Unit is None")
        if self.rec.get('updated_on') is None:
            raise ValueError("Updated on is None")
        if self.rec.get('source') is None:
            raise ValueError("Source is None")
        if self.rec.get('all_topics') is None:
            raise ValueError("All topics is None")