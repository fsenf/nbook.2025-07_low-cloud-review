import numpy as np
from satpy.scene import Scene
from satpy.resample import get_area_def 
from satpy.writers import get_enhanced_image
import glob
import matplotlib.pyplot as plt
import cartopy
import cartopy.crs as ccrs

#Germany scene
date="20250619"
custom_box=[6, 50, 12, 55] 
chunks="003[7-8]"  #these are the chunks over N Germany

# define path to FCI data folder
datadir="/work/bb1376/user/fabian/data/fci/fci_l1c_input_data/"
channel_name='true_color' # to select channel or RGB composite to visualize


# list of repeat cycles to use in the animation. These should be in the data
# downloaded from eumdac in datadir. One can use all data or a subset, 
# explicitly defining the RC number to use. Generally a daytime animation
# from ~6Z to ~19Z covers RCs numbers from 30 to 120.
RCs=np.arange(37,40)
for RC in RCs:
    path_to_data = datadir+"*BODY---*_{:04d}_".format(RC)+chunks+"*nc"
    print(path_to_data)
    # find files and assign the FCI reader
    # each FCI full disk repeat cycle contains 40 chunks.
    # to speed processing up we only use the chunks relevant to the ROI
    files = glob.glob(path_to_data)
    print(files)
    # create an FCI scene from the selected files.
    # the satpy reader automatically stitches the chunks
    scn_orig = Scene(reader='fci_l1c_nc',filenames=files)
    # available dataset names for this scene, e.g., 'vis_04', 'vis_05', ...
    #print(scn.available_dataset_names())
    # available composite names for this scene, e.g., 'natural_color',
    # 'airmass', 'convection', ...
    #print(scn_orig.available_composite_names())

    # load the datasets/composites of interest
    scn_orig.load([channel_name],upper_right_corner='NE')
    # resampling the channels to the highest available in the composite
    scn_res=scn_orig.resample(resampler="native")
    # crop to the required ROI
    scn= scn_res.crop(ll_bbox=custom_box)
    
    # handle 3-channel RGB
    if len(scn[channel_name].shape) == 3:
        values = get_enhanced_image(scn[channel_name]).data.values.transpose(1,2,0)
    else:
        values = scn[channel_name].values


    # set geolocation information
    adef = scn[channel_name].attrs['area']
    crs = adef.to_cartopy_crs()


    fig,axs = plt.subplots(ncols=1, figsize=(10, 10),subplot_kw={'projection': crs})
    axs.coastlines(resolution='10m')
    bodr = cartopy.feature.NaturalEarthFeature(category='cultural', 
                                               name='admin_0_boundary_lines_land', scale='50m', facecolor='none', alpha=0.6)
    axs.add_feature(bodr, linestyle='--', edgecolor='k', alpha=0.8)
    axs.set_axis_off()
    #adapt colormap ranges
    if 'vis' or 'nir' in channel_name:
        vmin = 0
        vmax = 40
    elif 'ir' or 'wv' in channel_name:
        vmin = 260
        vmax = 300
    else:
        vmin = vmax = None

    im = axs.imshow(values, transform=crs, extent=crs.bounds,
                    interpolation='none',aspect=1,vmin=vmin,vmax=vmax)
    axs.set_title("MTGI-1 FCI, "+channel_name+"\n"+str(scn[channel_name].attrs['end_time']))
    fig.tight_layout()
    plt.savefig("frame_{:04d}.png".format(RC))
    print("fig {:04d} done".format(RC))
#    plt.show()

# to create the animations:
import subprocess

subprocess.run('ffmpeg -hide_banner -loglevel error -framerate 10 -pattern_type glob -i "./*.png" -b:v 10000k -vcodec libx264 -vf scale=-2:1080 -pix_fmt yuv420p test.mp4 -y',shell=True)
