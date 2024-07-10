import chars2vec
import sklearn.decomposition
import matplotlib.pyplot as plt

# Load Inutition Engineering pretrained model
# Models names: 'eng_50', 'eng_100', 'eng_150', 'eng_200', 'eng_300'
c2v_model = chars2vec.load_model('eng_300')

words = ['Zaytoun riyad','Zeitoun Riad','Riad Zeitoun','Riad Zaytoun', 'Riyad Zaytoun', 'Riad Zaytoun',"Tony Mark","Tony Clark","Joseph Asslan","Joseph Aslan", "Josef Aslan","Joseph Aslan", "Josef Asln"]

# Preprocess words by splitting and sorting
preprocessed_words = [' '.join(sorted(word.split())) for word in words]

# Create word embeddings
word_embeddings = c2v_model.vectorize_words(preprocessed_words)

# Project embeddings on plane using the PCA
projection_2d = sklearn.decomposition.PCA(n_components=2).fit_transform(word_embeddings)

# Draw words on plane
f = plt.figure(figsize=(8, 6))

for j in range(len(projection_2d)):
    plt.scatter(projection_2d[j, 0], projection_2d[j, 1],
                marker=('$' + preprocessed_words[j] + '$'),
                s=500 * len(preprocessed_words[j]), label=j,
                facecolors='green' if 'Riad' in preprocessed_words[j] or 'Zeitoun' in preprocessed_words[j] else 'black')

plt.show()
