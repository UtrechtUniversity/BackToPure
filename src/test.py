

import pure_datasets


dataset = pure_datasets.find_dataset('f652e6b9-c253-44be-b3c6-757608c5c5cd', '10.6084/M9.FIGSHARE.21829182')
print (dataset)

for ui in dataset:
    print (ui)