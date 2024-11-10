import xml.etree.ElementTree as ET
import pandas as pd
import re
from tqdm import tqdm
from difflib import get_close_matches
import spacy 
import os


nlp = spacy.load('de_core_news_md')

def remove_protocol_end(protocol:str):
    t = re.split(r'\(Schluß der Sitzung:', protocol)

    if len(t) == 2:
        return t[0]
    
    t = re.split(r'\(Schluss:', protocol)

    if len(t) == 2:
        return t[0]
    
    t = re.split(r'\(Schluß der', protocol)

    if len(t) == 2:
        return t[0]
    
    t = re.split(r'\(Schluss der', protocol)

    if len(t) == 2:
        return t[0]

    t = re.split(r'\(Ende:', protocol)

    if len(t) == 2:
        return t[0]
    
    t = re.split(r'\(Sitzungsende:', protocol)

    if len(t) == 2:
        return t[0]

    raise ValueError('No end of protocol found' + str(len(t)))
def remove_protocol_start(protocol:str):
    t = re.split(r'\nBeginn: \d{1,2} ?[.:]\d{2}', protocol)

    if len(t) == 2:
        return t[1]
    
    else:
        raise ValueError('No start of protocol found')
    
def load_protocol(path):
    # open xml
    tree = ET.parse(path)
    root = tree.getroot()
    # get element 'TEXT'
    protocol = root[5].text
    return protocol

def get_interjections(protocol:str):
    '''
    get the interjections by non-speakers such as
    "(Beifall bei der CDU/CSU und der FDP)"
    
    '''

    interjection_idx = re.finditer(r'\n\(.*?\)', protocol, flags=re.DOTALL)
    return interjection_idx

def remove_interjections(protocol:str):
    '''
    remove the interjections by non-speakers such as
    "(Beifall bei der CDU/CSU und der FDP)"
    
    '''

    p = re.sub(r'\n\(.*?\)', '', protocol, flags=re.DOTALL)
    return p

def replace_other(protocol:str):
    '''
    replace other types of non-relevant patterns, e.g.

    "(A) (B) (C) (D)"
    "Deutscher Bundestag – 19 . Wahlperiode – 6 . Sitzung . Berlin, Mittwoch, den 17 . Januar 2018486"
    '''

    p = protocol.replace('(A)', '')
    p = p.replace('(B)', '')
    p = p.replace('(C)', '')
    p = p.replace('(D)', '')
    p = re.sub(r'Deutscher Bundestag .* \d\n', '', p)

    return p
    
def load_protocol(path) -> tuple[str, str, str]:
    # open xml
    tree = ET.parse(path)
    root = tree.getroot()
    # get element 'TEXT'
    protocol = root[5].text

    protocol_number = root[2].text
    date = root[3].text

    return protocol, protocol_number, date


def split_doc(protocol: str, doc: spacy.tokens.doc.Doc) -> list:
    entities = []
    for ent in doc.ents:
        # append all person
        if ent.label_ == 'PER':
            if ent.end_char != '\n':
                speaker_change = False
                if protocol[ent.end_char] == ':':
                    speaker_change = True

                # Leerzeichen und dann Partei
                elif len(re.findall(r' \(.*\):', protocol[ent.end_char:ent.end_char+30])) == 1:
                    # print(p[ent.end_char-20:ent.end_char+30], re.findall(r' \(.*\):', p[ent.end_char:ent.end_char+30]))
                    speaker_change = True

                # Leerzeichen Ort und dann Partei (z.B. 'Nun hat der Kollege Manfred Richter das Wort.\nManfred Richter (Bremerhaven) (F.D.P.): Frau Präsidentin!')
                elif len(re.findall(r' \(.*\) \(.*\):', protocol[ent.end_char:ent.end_char+30])) == 1:
                    speaker_change = True
                else:
                    speaker_change = False

                if speaker_change:
                    entities.append((ent.text, ent.start_char, ent.end_char, protocol[ent.start_char-10:ent.end_char+15], speaker_change, re.findall(r' \(.*\):', protocol[ent.end_char:ent.end_char+15])))
    return entities


def get_speaker_texts(protocol: str, parts: list) -> list:
    text_split = []
    for idx, ent in enumerate(parts):
        speaker = ent[0]
        speaker_start = ent[2]
        speaker_end = parts[idx+1][1] if idx+1 < len(parts) else len(protocol)
        text = protocol[speaker_start:speaker_end]

        if len(ent[5]) > 0:
            party = ent[5][0]
            party = re.findall(r'\(.*\)', party)[0]
            text = text.replace(ent[5][0], '')
        else:
            text = text.replace(':\n', '')

        text_split.append({
            'speaker':speaker, 
            'text': text, 
            'party':party if len(ent[5]) > 0 else None})
    return text_split


def merge_speakers(speakers: pd.DataFrame, speeches: pd.DataFrame, wp: pd.DataFrame, date:pd.Timestamp) -> pd.DataFrame:
    
    # nur Abgeordnete, die zum Zeitpunkt der Rede aktiv waren
    wp_subset = wp.loc[(wp['VON'] <= date) & (wp['BIS'] >= date)]
    speakers_subset = speakers.merge(wp_subset, left_on='ID', right_on='MDB_ID', how='inner')
    speakers_subset['full_name'] = speakers_subset['TITEL'].fillna('') + ' ' + speakers_subset['VORNAME'] + ' ' + speakers_subset['NACHNAME']

    # fuzzy match speaker names
    speeches['speaker_match'] = speeches['speaker'].apply(lambda x: get_close_matches(x, speakers_subset['full_name'], n=1, cutoff=0.5))
    speeches['speaker_match'] = speeches['speaker_match'].apply(lambda x: x[0] if len(x) > 0 else None)
    speeches_w_speaker_id = speeches.merge(speakers_subset[['full_name', 'ID']], left_on='speaker_match', right_on='full_name', how='left')

    return speeches_w_speaker_id


if __name__ == '__main__':
    DATA_PATH = '/home/laurinbrechter/Documents/Code/project-bundestag/data'
    PROTOCOLS_FOLDER = ['pp12', 'pp19']
    speakers = pd.read_csv(f'{DATA_PATH}/processed/mdb_stammdaten.csv')
    wp = pd.read_csv(f'{DATA_PATH}/processed/mdb_wahlperioden.csv')
    wp['VON'] = pd.to_datetime(wp['VON'], dayfirst=True)
    wp['BIS'] = pd.to_datetime(wp['BIS'], dayfirst=True)


    for folder in PROTOCOLS_FOLDER[:1]:
        path = f'{DATA_PATH}/{folder}'
        print(path)
        # get all files in folder
        files = os.listdir(path)
        for file in files[:1]:
            try:
                p, number, date = load_protocol(f'{path}/{file}')
                date = pd.Timestamp(date)
                p = remove_protocol_end(p)
                p = remove_protocol_start(p)
                p = remove_interjections(p)
                p = replace_other(p)

                doc = nlp(p)
                parts = split_doc(p, doc)
                text_split = get_speaker_texts(p, parts)
                speeches = pd.DataFrame(text_split)
                speeches = merge_speakers(speakers, speeches, wp, date)

                speeches_agg = speeches.groupby('full_name').agg(
                    full_text = ('text', ' '.join),
                    text_len = ('text', lambda x: len(' '.join(x).split())),
                    speaker_id = ('ID', 'first')
                )

                speeches_agg.full_text = speeches_agg.full_text.str.split(':').apply(lambda x: x[0] if len(x) == 1 else x[1])
                
                speeches_agg.to_csv(f'/home/laurinbrechter/Documents/Code/project-bundestag/data/processed/speeches.csv', index=False)

            except ValueError as e:
                print(e, file)
                continue